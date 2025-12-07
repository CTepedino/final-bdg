package core;

import exception.IllegalProgramArgumentException;
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

}