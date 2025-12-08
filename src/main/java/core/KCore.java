package core;

import exception.IllegalProgramArgumentException;
import exception.UnderlyingGraphException;
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

import static org.apache.spark.sql.functions.col;

public class KCore {

    public static void main(String[] args){

        if (args.length < 3) {
            System.out.println("Usage: core.KCore <vertices.csv> <edges.csv> <K>");
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
        boolean connectedKCore = true;
        if (args.length > 3){
            connectedKCore = Boolean.parseBoolean(args[3]);
        }

        SparkConf spark = new SparkConf().setAppName("KCore");
        JavaSparkContext sparkContext= new JavaSparkContext(spark);
        SparkSession session = SparkSession.builder()
            .sparkContext(sparkContext.sc())
            .getOrCreate();

        Dataset<Row> verticesDF = session.read()
            .option("header","true")
            .schema(loadSchemaVertices())
            .csv(verticesPath);

        Dataset<Row> edgesDF = session.read()
            .option("header","true")
            .schema(loadSchemaEdges())
            .csv(edgesPath);

        Dataset<Row> undirectedEdgesDF = getUndirectedEdges(edgesDF);

        GraphFrame graph = GraphFrame.apply(verticesDF, undirectedEdgesDF);

        GraphFrame kCore;
        if (connectedKCore){
            kCore = computeConnectedKCore(graph, k);
        } else {
            kCore = computeKCore(graph, k);
        }

        System.out.println(k + "-core vertices:");
        kCore.vertices().show();

        System.out.println("------------------------------------------------------------");
        System.out.println(k + "-core edges:");
        kCore.edges().show();

        saveToHDFS(kCore, session);

        session.close();
    }

    public static StructType loadSchemaVertices(){
        List<StructField> vertFields = new ArrayList<>();
        vertFields.add(DataTypes.createStructField("id", DataTypes.LongType, false));
        vertFields.add(DataTypes.createStructField("name", DataTypes.StringType, false));

        return DataTypes.createStructType(vertFields);
    }

    public static StructType loadSchemaEdges(){
        List<StructField> edgeFields = new ArrayList<>();
        edgeFields.add(DataTypes.createStructField("idSrc", DataTypes.LongType, false));
        edgeFields.add(DataTypes.createStructField("idDst", DataTypes.LongType, false));

        return DataTypes.createStructType(edgeFields);
    }

    public static Dataset<Row> getUndirectedEdges(Dataset<Row> edgesDF){
        Dataset<Row> undirectedEdgesDF = edgesDF
            .withColumn("src", functions.least(edgesDF.col("idSrc"), edgesDF.col("idDst")))
            .withColumn("dst", functions.greatest(edgesDF.col("idSrc"), edgesDF.col("idDst")))
            .select("src", "dst");

        Dataset<Row> duplicates = undirectedEdgesDF
            .groupBy("src", "dst")
            .count()
            .filter("count > 1");

        if (!duplicates.isEmpty()) {
            throw new UnderlyingGraphException("The underlying non-directed structure is a multigraph");
        }

        return undirectedEdgesDF;
    }

    public static GraphFrame computeKCore(GraphFrame graph, int k){
        GraphFrame currentGraph = graph;

        while (true){
            Dataset<Row> degrees = currentGraph.degrees();

            Dataset<Row> coreVertices = degrees
                    .filter("degree >= " + k)
                    .select("id");
            long remaining = coreVertices.count();

            if (remaining == currentGraph.vertices().count()) {
                return currentGraph;
            }

            Dataset<Row> remainingEdges = currentGraph.edges()
                    .join(coreVertices.withColumnRenamed("id", "src"), "src")
                    .join(coreVertices.withColumnRenamed("id", "dst"), "dst");

            Dataset<Row> remainingVertices = currentGraph.vertices()
                    .join(coreVertices, "id");

            currentGraph = GraphFrame.apply(remainingVertices, remainingEdges);
        }
    }

    public static GraphFrame computeConnectedKCore(GraphFrame graph, int k){
        GraphFrame disconnectedKCore = computeKCore(graph, k);

        Dataset<Row> comps = disconnectedKCore.connectedComponents().run();

        String largestComponent = comps
            .groupBy("component")
            .count()
            .orderBy(functions.desc("count"))
            .limit(1)
            .first()
            .getAs("component");

        Dataset<Row> largestVertices = comps
            .filter(col("component").equalTo(largestComponent))
            .select(col("id"));

        Dataset<Row> largestEdges = disconnectedKCore.edges()
            .join(largestVertices, col("src").equalTo(largestVertices.col("id")))
            .join(largestVertices, col("dst").equalTo(largestVertices.col("id")))
            .select("src", "dst");

        return GraphFrame.apply(largestVertices, largestEdges);
    }

    public static void saveToHDFS(GraphFrame graph, SparkSession session){
        String timestamp = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
                .withZone(ZoneOffset.UTC)
                .format(Instant.now());

        Configuration hadoopConf = session.sparkContext().hadoopConfiguration();

        try {
            FileSystem fs = FileSystem.get(hadoopConf);

            Path home = fs.getHomeDirectory();

            Path targetNodes = new Path(home, timestamp + "-nodes.csv");
            Path targetEdges = new Path(home, timestamp + "-edges.csv");

            Path tmpNodesDir = new Path(home, ".kcore_tmp_" + timestamp + "_nodes");
            Path tmpEdgesDir = new Path(home, ".kcore_tmp_" + timestamp + "_edges");

            graph.vertices()
                    .coalesce(1)
                    .write()
                    .option("header", "true")
                    .mode("overwrite")
                    .csv(tmpNodesDir.toString());

            graph.edges()
                    .withColumnRenamed("src", "idSrc")
                    .withColumnRenamed("dst", "idDst")
                    .coalesce(1)
                    .write()
                    .option("header", "true")
                    .mode("overwrite")
                    .csv(tmpEdgesDir.toString());

            movePartToTarget(fs, tmpNodesDir, targetNodes);
            movePartToTarget(fs, tmpEdgesDir, targetEdges);

            fs.delete(tmpNodesDir, true);
            fs.delete(tmpEdgesDir, true);


        } catch (IOException e){
            throw new RuntimeException(e);
        }

    }

    public static void movePartToTarget(FileSystem fs, Path tmpDir, Path target) throws IOException {
        FileStatus[] status = fs.listStatus(tmpDir, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                String name = path.getName();
                return name.startsWith("part-") && name.endsWith(".csv");
            }
        });

        if (status == null || status.length == 0) {
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
            if (fs.exists(target)) {
                fs.delete(target, false);
            }
            fs.rename(part, target);
            return;
        }

        Path partFile = status[0].getPath();

        if (fs.exists(target)) {
            fs.delete(target, false);
        }

        boolean ok = fs.rename(partFile, target);
        if (!ok) {
            throw new IOException("No se pudo mover " + partFile + " a " + target);
        }
    }
}