package core;

import exception.IllegalProgramArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class KCore {

    public static void main(String[] args){

        if (args.length < 3) {
            System.err.println("Usage: core.KCore <vertices.csv> <edges.csv> <K>");
            System.exit(1);
        }

        String verticesPath = args[0];
        String edgesPath = args[1];
        int k;
        try {
            k = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            throw new IllegalProgramArgumentException("K must be an integer. Provided: " + args[2]);
        }

        if (k < 1) {
            throw new IllegalProgramArgumentException("K must be >= 1");
        }

        SparkConf spark = new SparkConf().setAppName("KCore");
        JavaSparkContext sparkContext= new JavaSparkContext(spark);
        SparkSession session = SparkSession.builder()
                .sparkContext(sparkContext.sc())
                .getOrCreate();

        Dataset<Row> verticesDF = session.read()
                .option("header","true")
                .schema(LoadSchemaVertices())
                .csv(verticesPath);

        Dataset<Row> edgesDF = session.read()
                .option("header","true")
                .schema(LoadSchemaEdges())
                .csv(edgesPath);

        Dataset<Row> undirectedEdges =
                edgesDF
                    .withColumn("src_ord", functions.least(edgesDF.col("src"), edgesDF.col("dst")))
                    .withColumn("dst_ord", functions.greatest(edgesDF.col("src"), edgesDF.col("dst")))
                    .select("src_ord", "dst_ord")
                    .withColumnRenamed("src_ord", "src")
                    .withColumnRenamed("dst_ord", "dst");

        Dataset<Row> duplicates =
                undirectedEdges.groupBy("src", "dst").count()
                        .filter("count > 1");

        if (duplicates.count() > 0) {
            throw new RuntimeException("El grafo es un multigrafo: existen aristas duplicadas.");
        }

        GraphFrame undirectedGraph = GraphFrame.apply(verticesDF, undirectedEdges);

        GraphFrame kcore = computeKCore(undirectedGraph, k);

        if (kcore == null || kcore.vertices().count() == 0) {
            System.out.println("El " + k + "-núcleo es vacío.");
        } else {
            System.out.println(k +"-core vertices:");
            kcore.vertices().show(false);

            System.out.println(k +"-core edges:");
            kcore.edges().show(false);

            // -----------------------------------------------------
            // Guardar en HDFS en el home del usuario: <timestamp>-nodes.csv y <timestamp>-edges.csv
            String timestamp = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
                    .withZone(ZoneOffset.UTC)
                    .format(Instant.now());

            try {

                saveKCoreToHdfs(kcore, timestamp, session);
            } catch (IOException e) {
                throw new RuntimeException("Error al guardar K-core en HDFS: " + e.getMessage(), e);
            }
        }

        // good practice: detener session y context
        session.stop();
        sparkContext.close();

        session.close();
    }

    public static StructType LoadSchemaVertices() {
        List<StructField> vertFields = new ArrayList<>();
        vertFields.add(DataTypes.createStructField("id",DataTypes.LongType, true));
        vertFields.add(DataTypes.createStructField("name",DataTypes.StringType, true));

        return DataTypes.createStructType(vertFields);
    }


    public static StructType LoadSchemaEdges() {
        List<StructField> edgeFields = new ArrayList<>();
        edgeFields.add(DataTypes.createStructField("src",DataTypes.LongType, false));
        edgeFields.add(DataTypes.createStructField("dst",DataTypes.LongType, false));
        return DataTypes.createStructType(edgeFields);
    }


    public static GraphFrame computeKCore(GraphFrame graph, int k) {
        GraphFrame currentGraph = graph;

        while (true) {
            // Calcular el grado de los nodos
            Dataset<Row> degrees = currentGraph.degrees();

            // Filtrar los que cumplen el K
            Dataset<Row> coreVertices = degrees.filter("degree >= " + k)
                    .select("id");

            long remaining = coreVertices.count();

            if (remaining == 0) {
                // No hay K-core
                return null;
            }

            // Verificamos si no cambió la cantidad de vértices del grafo anterior
            if (remaining == currentGraph.vertices().count()) {
                // Converge → ya es K-core
                return currentGraph;
            }

            // Filtrar aristas para que ambos extremos sigan dentro
            Dataset<Row> newEdges =
                    currentGraph.edges()
                            .join(coreVertices.withColumnRenamed("id", "src"), "src")
                            .join(coreVertices.withColumnRenamed("id", "dst"), "dst");

            Dataset<Row> newVertices =
                    currentGraph.vertices()
                            .join(coreVertices, "id");

            currentGraph = GraphFrame.apply(newVertices, newEdges);
        }
    }

    // ----------------------------------------------------------------
    // Guarda los DataFrames en HDFS como un único CSV cada uno,
    // renombrando el part-*.csv a <timestamp>-nodes.csv y <timestamp>-edges.csv
    public static void saveKCoreToHdfs(GraphFrame kcore, String timestamp, SparkSession session) throws IOException {
        // Obtenemos la configuración Hadoop usada por Spark
        Configuration hadoopConf = session.sparkContext().hadoopConfiguration();
        FileSystem fs = FileSystem.get(hadoopConf);

        // Home del usuario en HDFS (respetando la configuración del cluster)
        Path home = fs.getHomeDirectory();

        // Nombres finales
        Path targetNodes = new Path(home, timestamp + "-nodes.csv");
        Path targetEdges = new Path(home, timestamp + "-edges.csv");

        // Temp dirs (ocultos) para escribir con Spark (coalesce para un único part)
        Path tmpNodesDir = new Path(home, ".kcore_tmp_" + timestamp + "_nodes");
        Path tmpEdgesDir = new Path(home, ".kcore_tmp_" + timestamp + "_edges");

        // Escribir vertices
        kcore.vertices()
                .coalesce(1)
                .write()
                .option("header", "true")
                .mode("overwrite")
                .csv(tmpNodesDir.toString());

        // Escribir edges
        kcore.edges()
                .coalesce(1)
                .write()
                .option("header", "true")
                .mode("overwrite")
                .csv(tmpEdgesDir.toString());

        // Mover/renombrar part-*.csv a destino final
        movePartToTarget(fs, tmpNodesDir, targetNodes);
        movePartToTarget(fs, tmpEdgesDir, targetEdges);

        // borrar temporales
        fs.delete(tmpNodesDir, true);
        fs.delete(tmpEdgesDir, true);

        System.out.println("K-core saved to HDFS:");
        System.out.println(" - " + targetNodes.toString());
        System.out.println(" - " + targetEdges.toString());
    }


    private static void movePartToTarget(FileSystem fs, Path tmpDir, Path target) throws IOException {
        // listar ficheros en el directorio temporal que empiecen por part- y terminen en .csv
        FileStatus[] status = fs.listStatus(tmpDir, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                String name = path.getName();
                return name.startsWith("part-") && name.endsWith(".csv");
            }
        });

        if (status == null || status.length == 0) {
            // en algunos clusters la extensión puede ser .csv or sin extension; listar todo y buscar part-
            FileStatus[] all = fs.listStatus(tmpDir);
            Path part = null;
            for (FileStatus f : all) {
                if (f.getPath().getName().startsWith("part-")) {
                    part = f.getPath();
                    break;
                }
            }
            if (part == null) {
                throw new IOException("No se encontró archivo part-*.csv en " + tmpDir);
            }
            // renombrar
            if (fs.exists(target)) {
                fs.delete(target, false);
            }
            fs.rename(part, target);
            return;
        }

        // tomar el primero
        Path partFile = status[0].getPath();

        // Si ya existe target, lo removemos primero
        if (fs.exists(target)) {
            fs.delete(target, false);
        }

        boolean ok = fs.rename(partFile, target);
        if (!ok) {
            throw new IOException("No se pudo mover " + partFile + " a " + target);
        }
    }

}