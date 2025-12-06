package ar.edu.itba.bdg;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class TestMain {

    public static void main(String[] args){

        if (args.length < 3){
            System.out.println("Missing one or more required parameters: <Vertices File> <Edges File> <K>");
            return;
        }



        SparkConf spark = new SparkConf().setAppName("K-cores");
        JavaSparkContext sparkContext = new JavaSparkContext(spark);
        SparkSession session = SparkSession.builder()
                .sparkContext(sparkContext.sc())
                .getOrCreate();
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(session);

        List<Row> vertices = LoadVertices(args[0]);
        Dataset<Row> verticesDF = sqlContext.createDataFrame( vertices, LoadSchemaVertices() );

        List<Row> edges = LoadEdges(args[1]);
        Dataset<Row> edgesDF = sqlContext.createDataFrame( edges, LoadSchemaEdges() );



        GraphFrame graph = GraphFrame.apply(verticesDF, edgesDF);


        System.out.println("Graph:");
        graph.vertices().show();
        graph.edges().show();


        System.out.println("Schema");
        graph.vertices().printSchema();
        graph.edges().printSchema();

        System.out.println("Degree");
        graph.degrees().show();

        System.out.println("Indegree");
        graph.inDegrees().show();

        System.out.println("Outdegree");
        graph.outDegrees().show();

        sparkContext.close();
    }

    public static List<Row> LoadVertices(String filePath) {
        List<Row> rows = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            br.readLine();

            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                rows.add(RowFactory.create(Long.parseLong(parts[0]), parts[1]));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return rows;
    }

    public static StructType LoadSchemaVertices(){
        List<StructField> vertFields = new ArrayList<>();

        vertFields.add(DataTypes.createStructField("id",DataTypes.LongType, false));
        vertFields.add(DataTypes.createStructField("name",DataTypes.StringType, false));

        return DataTypes.createStructType(vertFields);
    }

    public static List<Row> LoadEdges(String filePath) {
        List<Row> rows = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            br.readLine();

            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                rows.add(RowFactory.create(Long.parseLong(parts[0]), Long.parseLong(parts[1])));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return rows;
    }

    public static StructType LoadSchemaEdges(){
        List<StructField> edgeFields = new ArrayList<>();
        edgeFields.add(DataTypes.createStructField("src",DataTypes.LongType, false));
        edgeFields.add(DataTypes.createStructField("dst",DataTypes.LongType, false));

        return DataTypes.createStructType(edgeFields);
    }
}
