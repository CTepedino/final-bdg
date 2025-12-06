package ar.edu.itba.bdg;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class KCoresAppMain {

    public static void main(String[] args) throws ParseException {

        SparkConf spark = new SparkConf().setAppName("K-cores");
        JavaSparkContext sparkContext = new JavaSparkContext(spark);
        SparkSession session = SparkSession.builder()
                .sparkContext(sparkContext.sc())
                .getOrCreate();

        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(session);

        List<Row> vertices = LoadVertices();
        Dataset<Row> verticesDF = sqlContext.createDataFrame( vertices, LoadSchemaVertices() );

        List<Row> edges = LoadEdges();
        Dataset<Row> edgesDF = sqlContext.createDataFrame( edges, LoadSchemaEdges() );


        GraphFrame graph = GraphFrame.apply(verticesDF, edgesDF);


        // in the driver console
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


    public static StructType LoadSchemaVertices()
    {
        // metadata
        List<StructField> vertFields = new ArrayList<StructField>();
        vertFields.add(DataTypes.createStructField("id",DataTypes.LongType, true));
        vertFields.add(DataTypes.createStructField("URL",DataTypes.StringType, true));
        vertFields.add(DataTypes.createStructField("owner",DataTypes.StringType, true));
        vertFields.add(DataTypes.createStructField("year",DataTypes.IntegerType, true));

        StructType schema = DataTypes.createStructType(vertFields);

        return schema;
    }

    public static StructType LoadSchemaEdges()
    {
        // metadata
        List<StructField> edgeFields = new ArrayList<StructField>();
        edgeFields.add(DataTypes.createStructField("src",DataTypes.LongType, false));
        edgeFields.add(DataTypes.createStructField("dst",DataTypes.LongType, false));
        edgeFields.add(DataTypes.createStructField("label",DataTypes.StringType, false));
        edgeFields.add(DataTypes.createStructField("creationDate",DataTypes.DateType, true));

        StructType schema = DataTypes.createStructType(edgeFields);

        return schema;
    }

    public static List<Row> LoadVerticesV1()
    {
        ArrayList<Row> vertices = new ArrayList<Row>();

        for(long i= 0; i < 10; i++)
            vertices.add(RowFactory.create(i, i + ".html",
                    i % 2 == 0? "A": "L") );

        return vertices;
    }

    public static List<Row> LoadVertices()
    {
        int year[]= new int[] { 2009, 2008, 2010, 2009, 2009, 2007, 2008, 2009, 2009, 2009};

        ArrayList<Row> vertList = new ArrayList<Row>();

        for(long rec= 0; rec <= 9; rec++)
        {
            vertList.add(RowFactory.create( rec, rec  + ".html", rec%2==0?"A":"L", year[(int)rec] ));
        }

        return vertList;
    }

    public static List<Row>  LoadEdgesV1() throws ParseException
    {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");

        ArrayList<Row> edges = new ArrayList<Row>();


        edges.add(RowFactory.create(1L, 0L, "refersTo", new java.sql.Date(sdf.parse("10/10/2010").getTime()) ));
        edges.add(RowFactory.create(1L, 0L, "refersTo", new java.sql.Date(sdf.parse("11/10/2010").getTime()) ));  // new one
        edges.add(RowFactory.create(1L, 0L, "refersTo", new java.sql.Date(sdf.parse("12/10/2010").getTime()) ));  // new one

        edges.add(RowFactory.create(1L, 2L, "refersTo", new java.sql.Date(sdf.parse("10/10/2010").getTime()) ));

        edges.add(RowFactory.create(2L, 0L, "refersTo", new java.sql.Date(sdf.parse("10/10/2010").getTime()) ));

        edges.add(RowFactory.create(3L, 2L, "refersTo", new java.sql.Date(sdf.parse("6/10/2010").getTime()) ));
        edges.add(RowFactory.create(3L, 2L, "refersTo", new java.sql.Date(sdf.parse("7/10/2010").getTime()) ));  // new one
        edges.add(RowFactory.create(3L, 2L, "refersTo", new java.sql.Date(sdf.parse("7/10/2012").getTime()) ));  // new one


        edges.add(RowFactory.create(3L, 6L, "refersTo", new java.sql.Date(sdf.parse("15/10/2010").getTime()) ));

        edges.add(RowFactory.create(4L, 3L, "refersTo", new java.sql.Date(sdf.parse("15/10/2010" ).getTime()) ));

        edges.add(RowFactory.create(5L, 4L, "refersTo", new java.sql.Date(sdf.parse("21/10/2010").getTime()) ));
        edges.add(RowFactory.create(6L, 4L, "refersTo", new java.sql.Date(sdf.parse("10/10/2010").getTime()) ));
        edges.add(RowFactory.create(7L, 6L, "refersTo", new java.sql.Date(sdf.parse("10/10/2010").getTime()) ));
        edges.add(RowFactory.create(7L, 8L, "refersTo", new java.sql.Date(sdf.parse("15/10/2010").getTime()) ));
        edges.add(RowFactory.create(8L, 5L, "refersTo", new java.sql.Date(sdf.parse("21/10/2010").getTime()) ));

        edges.add(RowFactory.create(8L, 6L, "refersTo", new java.sql.Date(sdf.parse("15/10/2010").getTime()) ));
        edges.add(RowFactory.create(8L, 6L, "refersTo", new java.sql.Date(sdf.parse("18/10/2010").getTime()) ));  // NEW ONE
        edges.add(RowFactory.create(8L, 6L, "refersTo", new java.sql.Date(sdf.parse("21/10/2010").getTime()) )); // NEW ONE
        edges.add(RowFactory.create(8L, 6L, "refersTo", new java.sql.Date(sdf.parse("23/10/2010").getTime()) ));  // NEW ONE


        return edges;
    }


    public static List<Row>  LoadEdges() throws ParseException
    {
        ArrayList<Row> edges = new ArrayList<Row>();
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");


        edges.add(RowFactory.create( 1L, 0L, "refersTo", new java.sql.Date(sdf.parse("10/10/2009").getTime() )));
        edges.add(RowFactory.create( 1L, 2L, "refersTo", new java.sql.Date(sdf.parse("10/10/2009").getTime())));
        edges.add(RowFactory.create( 2L, 0L, "refersTo", new java.sql.Date(sdf.parse("10/10/2010").getTime())));
        edges.add(RowFactory.create( 3L, 2L, "refersTo", new java.sql.Date(sdf.parse("10/10/2010").getTime())));
        edges.add(RowFactory.create( 3L, 6L, "refersTo", new java.sql.Date(sdf.parse("15/10/2009").getTime())));
        edges.add(RowFactory.create( 4L, 3L, "refersTo", new java.sql.Date(sdf.parse("15/10/2012").getTime())));
        edges.add(RowFactory.create( 5L, 4L,  "refersTo", new java.sql.Date(sdf.parse("21/10/2010").getTime())));
        edges.add(RowFactory.create( 6L, 4L, "refersTo", new java.sql.Date(sdf.parse("10/10/2010").getTime())));
        edges.add(RowFactory.create( 7L, 6L, "refersTo", new java.sql.Date(sdf.parse("10/10/2010").getTime())));
        edges.add(RowFactory.create( 7L, 8L, "refersTo", new java.sql.Date(sdf.parse("15/10/2010").getTime())));
        edges.add(RowFactory.create( 8L, 5L, "refersTo", new java.sql.Date(sdf.parse("21/10/2010").getTime())));
        edges.add(RowFactory.create( 8L, 6L, "refersTo", new java.sql.Date(sdf.parse("15/10/2010").getTime())));

        return edges;

    }



}
