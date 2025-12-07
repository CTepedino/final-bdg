package core;

import exception.IllegalProgramArgumentException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KCore {

    public static void main(String[] args) {

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
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(session);

        List<Row> vertices = LoadVertices(verticesPath);
        Dataset<Row> verticesDF = sqlContext.createDataFrame( vertices, LoadSchemaVertices());

        List<Row> edges = LoadEdges(edgesPath);
        Dataset<Row> edgesDF = sqlContext.createDataFrame( edges, LoadSchemaEdges());

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

        session.close();
    }

    public static List<Row> LoadVertices(String filePath) {
        List<Row> vertices = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {

            reader.lines()
                    .skip(1) //skip header
                    .forEach(line -> {
                        String[] values = line.split(",", 2);
                        vertices.add(RowFactory.create(
                                Long.parseLong(values[0]),
                                values[1]
                        ));
                    });

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return vertices;
    }

    public static List<Row> LoadEdges(String filePath){
        List<Row> edges = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {

            reader.lines()
                    .skip(1) //skip header
                    .forEach(line -> {
                        String[] values = line.split(",", 2);
                        edges.add(RowFactory.create(
                                Long.parseLong(values[0]),
                                Long.parseLong(values[1])
                        ));
                    });

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return edges;
    }

    public static StructType LoadSchemaVertices(){
        // metadata
        List<StructField> vertFields = new ArrayList<>();
        vertFields.add(DataTypes.createStructField("id",DataTypes.LongType, true));
        vertFields.add(DataTypes.createStructField("name",DataTypes.StringType, true));

        return DataTypes.createStructType(vertFields);
    }

    public static StructType LoadSchemaEdges(){
        // metadata
        List<StructField> edgeFields = new ArrayList<>();
        edgeFields.add(DataTypes.createStructField("src", DataTypes.LongType, false));
        edgeFields.add(DataTypes.createStructField("dst", DataTypes.LongType, false));

        return DataTypes.createStructType(edgeFields);
    }


}