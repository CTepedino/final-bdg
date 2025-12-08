package core;

import exception.IllegalProgramArgumentException;
import exception.UnderlyingGraphException;
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

import java.util.ArrayList;
import java.util.List;

public class KCoreGraphFrames {

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

        GraphFrame kCore = computeKCore(graph, k);


        System.out.println(k + "-core vertices:");
        kCore.vertices().show();

        System.out.println("------------------------------------------------------------");
        System.out.println(k + "-core edges:");
        kCore.edges().show();

       // HDFSUtils.saveToHDFS(kCore, session);

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

        if (duplicates.limit(1).count() != 0) {
            throw new UnderlyingGraphException("The underlying non-directed structure is a multigraph");
        }

        return undirectedEdgesDF;
    }

    public static GraphFrame computeKCore(GraphFrame graph, int k){
        GraphFrame currentGraph = graph;
        long prevRemaining = currentGraph.vertices().count();

        while (true){
            Dataset<Row> degrees = currentGraph.degrees();

            Dataset<Row> coreVertices = degrees
                    .filter("degree >= " + k)
                    .select("id");

            long remaining = coreVertices.count();

            if (remaining == prevRemaining) {
                return currentGraph;
            }

            prevRemaining = remaining;

            Dataset<Row> remainingEdges = currentGraph.edges()
                    .join(coreVertices.withColumnRenamed("id", "src"), "src")
                    .join(coreVertices.withColumnRenamed("id", "dst"), "dst");

            Dataset<Row> remainingVertices = currentGraph.vertices()
                    .join(coreVertices, "id");

            currentGraph = GraphFrame.apply(remainingVertices, remainingEdges);
        }
    }

}