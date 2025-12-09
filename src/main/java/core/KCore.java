package core;

import exception.IllegalProgramArgumentException;
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

public class KCore {

    private static final ClassTag<String> STRING_TAG = ClassTag$.MODULE$.apply(String.class);

    public static void main(String[] args){
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

        System.out.println("vertices:");
        kCore.vertices().toJavaRDD().collect().forEach(System.out::println);

        System.out.println();

        System.out.println("edges:");
        kCore.edges().toJavaRDD().collect().forEach(System.out::println);


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


        JavaRDD<Edge<String>> uniqueEdges = normalizedEdges
            .mapToPair(e -> new Tuple2<>(new Tuple2<>(e.srcId(), e.dstId()), e))
            .reduceByKey((a, b) -> a)
            .values();

        return uniqueEdges;
    }

    public static Graph<String, String> computeKCore(Graph<String, String> graph, int k) {

        Graph<String, String> current = graph;
        long previousRemaining = current.vertices().count();

        while (true) {
            VertexRDD<Object> degreeRDD = current.ops().degrees();

            JavaPairRDD<Long, Long> validVertices =
                    degreeRDD.toJavaRDD()
                            .mapToPair(v -> new Tuple2<>(
                                    ((Number) v._1()).longValue(),
                                    ((Number) v._2()).longValue())
                            )
                            .filter(v -> v._2 >= k);

            long remaining = validVertices.count();
            if (remaining == previousRemaining) {
                return current;
            }
            previousRemaining = remaining;


            JavaPairRDD<Long, String> currentVertices =
                    current.vertices().toJavaRDD().mapToPair(v ->
                            new Tuple2<>(
                                    ((Number) v._1()).longValue(),
                                    v._2()
                            )
                    );

            JavaRDD<Tuple2<Object, String>> remainingVerticesRDD =
                    currentVertices.join(validVertices)
                            .map(t -> new Tuple2<Object, String>(t._1, t._2._1));

            JavaPairRDD<Long, Edge<String>> survivingBySrc =
                    current.edges().toJavaRDD()
                            .mapToPair(e -> new Tuple2<>(e.srcId(), e))
                            .join(validVertices)
                            .mapToPair(t -> new Tuple2<>(t._2._1.dstId(), t._2._1));

            JavaRDD<Edge<String>> remainingEdgesRDD =
                    survivingBySrc.join(validVertices)
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
    }

}
