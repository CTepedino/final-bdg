package core;

import exception.IllegalProgramArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;

public class KCore {

    private static final ClassTag<String> STRING_TAG = ClassTag$.MODULE$.apply(String.class);

    public static void main(String[] args){
        String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());

        if (args.length < 3) {
            System.err.println("Usage: KCore <vertices-file> <edges-file> <k-value>");
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
            throw new IllegalProgramArgumentException("K must be >= 1. Provided: " + k);
        }

        SparkConf spark = new SparkConf().setAppName("KCore");
        JavaSparkContext sparkContext = new JavaSparkContext(spark);

        JavaRDD<Tuple2<Object, String>> verticesRDD = LoadVertices(sparkContext, verticesPath);
        JavaRDD<Edge<String>> edgesRDD = LoadEdges(sparkContext, edgesPath);

        JavaRDD<Edge<String>> undirectedEdgesRDD = getUndirectedEdges(edgesRDD);

        Graph<String, String> graph = Graph.apply(
                verticesRDD.rdd(),
                undirectedEdgesRDD.rdd(),
                "default",
                StorageLevel.MEMORY_ONLY(),
                StorageLevel.MEMORY_ONLY(),
                STRING_TAG,
                STRING_TAG
        );

        Graph<String, String> kCore = computeKCore(graph, k);

        System.out.println(k + "-core graph");
        System.out.println("\n\n");

        System.out.println("vertices");
        System.out.println("id,name");
        kCore.vertices().toJavaRDD().collect().forEach(System.out::println);

        System.out.println("\n\n");

        System.out.println("edges");
        System.out.println("idSrc,idDst");
        kCore.edges().toJavaRDD().collect().forEach(e -> System.out.println("(" + e.srcId() + "," + e.dstId() + ")"));

        saveToHDFS(kCore, timestamp, sparkContext);


        sparkContext.close();
    }

    public static JavaRDD<Tuple2<Object, String>> LoadVertices(JavaSparkContext sc, String filePath) {
        JavaRDD<String> lines = sc.textFile(filePath);
        String header = lines.first();

        return lines.filter(line -> !line.equals(header))
                .map(line -> {
                    String[] parts = line.split(",");
                    Long id = Long.parseLong(parts[0]);
                    String label = parts[1];
                    return new Tuple2<>(id, label);
                });
    }

    public static JavaRDD<Edge<String>> LoadEdges(JavaSparkContext sc, String filePath) {
        JavaRDD<String> lines = sc.textFile(filePath);
        String header = lines.first();

        return lines.filter(line -> !line.equals(header))
                .map(line -> {
                    String[] parts = line.split(",");
                    long srcId = Long.parseLong(parts[0]);
                    long dstId = Long.parseLong(parts[1]);
                    return new Edge<>(srcId, dstId, "rel");
                });
    }

    public static JavaRDD<Edge<String>> getUndirectedEdges(JavaRDD<Edge<String>> edgesRDD) {

        JavaRDD<Edge<String>> normalizedEdges = edgesRDD.map(e -> {
            long src = e.srcId();
            long dst = e.dstId();
            if (src <= dst) {
                return new Edge<>(src, dst, e.attr());
            } else {
                return new Edge<>(dst, src, e.attr());
            }
        });

        long duplicateCount = normalizedEdges
            .mapToPair(e -> new Tuple2<>(new Tuple2<>(e.srcId(), e.dstId()), 1L))
            .reduceByKey(Long::sum)
            .filter(t -> t._2 > 1)
            .count();

        if (duplicateCount > 0) {
            throw new RuntimeException("The underlying non-directed structure is a multigraph");
        }

        return normalizedEdges
            .mapToPair(e -> new Tuple2<>(new Tuple2<>(e.srcId(), e.dstId()), e))
            .reduceByKey((a, b) -> a)
            .values();
    }

    public static Graph<String, String> computeKCore(Graph<String, String> graph, int k) {

        Graph<String, String> current = graph;
        long previousRemaining = current.vertices().count();

        while (true) {
            VertexRDD<Object> degreeRDD = current.ops().degrees();

            JavaPairRDD<Long, Long> validVertices = degreeRDD.toJavaRDD()
                .mapToPair(v -> new Tuple2<>(
                    ((Number) v._1()).longValue(),
                    ((Number) v._2()).longValue())
                )
                .filter(v -> v._2 >= k);

            long remaining = validVertices.count();
            if (remaining == previousRemaining) {
                break;
            }
            previousRemaining = remaining;


            JavaPairRDD<Long, String> currentVertices = current.vertices().toJavaRDD()
                .mapToPair(v -> new Tuple2<>(
                    ((Number) v._1()).longValue(),
                    v._2()
                ));

            JavaRDD<Tuple2<Object, String>> remainingVerticesRDD = currentVertices
                .join(validVertices)
                .map(t -> new Tuple2<>(t._1, t._2._1));

            JavaPairRDD<Long, Edge<String>> survivingBySrc = current.edges().toJavaRDD()
                .mapToPair(e -> new Tuple2<>(e.srcId(), e))
                .join(validVertices)
                .mapToPair(t -> new Tuple2<>(t._2._1.dstId(), t._2._1));

            JavaRDD<Edge<String>> remainingEdgesRDD = survivingBySrc
                .join(validVertices)
                .map(t -> t._2._1);

            current = Graph.apply(
                JavaRDD.toRDD(remainingVerticesRDD),
                JavaRDD.toRDD(remainingEdgesRDD),
                "default",
                StorageLevel.MEMORY_ONLY(),
                StorageLevel.MEMORY_ONLY(),
                STRING_TAG,
                STRING_TAG
            );
        }

        return largestConnectedComponent(current);
    }

    public static Graph<String, String> largestConnectedComponent(Graph<String, String> graph) {

        Graph<Object, String> ccGraph = graph.ops().connectedComponents();

        JavaPairRDD<Object, Long> componentCounts = ccGraph.vertices().toJavaRDD()
            .mapToPair(v -> new Tuple2<>(v._2(), 1L))
            .reduceByKey(Long::sum);

        Tuple2<Object, Long> largest = componentCounts
            .max(Comparator.comparingLong(Tuple2::_2));

        Object mainComponentId = largest._1();

        JavaRDD<Tuple2<Object, String>> filteredVertices = ccGraph.vertices().toJavaRDD()
            .filter(v -> v._2().equals(mainComponentId))
            .map(v -> new Tuple2<>(v._1(), graph.vertices().toJavaRDD()
                .filter(orig -> orig._1().equals(v._1()))
                .first()._2())
            );

        JavaRDD<Edge<String>> filteredEdges = graph.edges().toJavaRDD()
            .filter(e ->
                    filteredVertices.map(Tuple2::_1).collect().contains(e.srcId()) &&
                    filteredVertices.map(Tuple2::_1).collect().contains(e.dstId())
            );

        return Graph.apply(
                JavaRDD.toRDD(filteredVertices),
                JavaRDD.toRDD(filteredEdges),
                "default",
                StorageLevel.MEMORY_ONLY(),
                StorageLevel.MEMORY_ONLY(),
                STRING_TAG,
                STRING_TAG
        );
    }

    public static void saveToHDFS(Graph<String, String> graph, String timestamp, JavaSparkContext jsc) {
        try {
            String hdfsHome = "hdfs:///user/" + System.getProperty("user.name") + "/";

            String verticesPathStr = hdfsHome + timestamp + "-nodes.csv";
            String edgesPathStr = hdfsHome + timestamp + "_edges.csv";

            Configuration conf = jsc.hadoopConfiguration();
            FileSystem fs = FileSystem.get(new URI("hdfs:///"), conf);

            Path verticesPath = new Path(verticesPathStr);
            if (fs.exists(verticesPath)) fs.delete(verticesPath, true);

            Path edgesPath = new Path(edgesPathStr);
            if (fs.exists(edgesPath)) fs.delete(edgesPath, true);

            JavaRDD<String> verticesRDD = graph.vertices().toJavaRDD()
                    .map(v -> v._1 + "," + v._2);
            verticesRDD.coalesce(1).saveAsTextFile(verticesPathStr);


            JavaRDD<String> edgesRDD = graph.edges().toJavaRDD()
                    .map(e -> e.srcId() + "," + e.dstId() + "," + e.attr());
            edgesRDD.coalesce(1).saveAsTextFile(edgesPathStr);
        } catch (Exception e) {
            System.err.println("Error saving graph in HDFS: " + e.getMessage());
        }
    }
}
