package core;

import exception.IllegalProgramArgumentException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeDirection;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Serializable;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class KCore {

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

        ClassTag<String> stringTag = ClassTag$.MODULE$.apply(String.class);

        Graph<String, String> graph = Graph.apply(
                verticesRDD.rdd(),
                undirectedEdgesRDD.rdd(),
                "default",
                StorageLevel.MEMORY_ONLY(),
                StorageLevel.MEMORY_ONLY(),
                stringTag,
                stringTag);

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
        long prevRemaining = current.vertices().count();

        // ClassTag para Graph.apply(...)
        ClassTag<String> tagString = ClassTag$.MODULE$.apply(String.class);

        while (true) {
            // 1) grados por vértice (VertexRDD<Object>)
            VertexRDD<Object> degrees = current.ops().degrees();

            // 2) convertir a JavaPairRDD<Long, Long> (vertexId -> degree) y filtrar degree >= k
            JavaPairRDD<Long, Long> validVertices = degrees.toJavaRDD()
                    .mapToPair(t -> {
                        // t._1() = vertexId (Object), t._2() = degree (Object)
                        Long vid = ((Number) t._1()).longValue();
                        Long deg = ((Number) t._2()).longValue();
                        return new Tuple2<>(vid, deg);
                    })
                    .filter(t -> t._2 >= k);

            long remaining = validVertices.count();

            // condición de convergencia
            if (remaining == prevRemaining) {
                return current;
            }
            prevRemaining = remaining;

            // 3) remainingVertices: hacer join entre current.vertices() y validVertices (distribuido)
            // current.vertices().toJavaRDD() produce JavaRDD<Tuple2<Object, String>>
            JavaPairRDD<Long, String> verticesById = current.vertices().toJavaRDD()
                    .mapToPair(v -> {
                        Long vid = ((Number) v._1()).longValue();
                        String attr = v._2();
                        return new Tuple2<>(vid, attr);
                    });

            // join garantiza que nos quedamos sólo con los vértices válidos
            JavaPairRDD<Long, Tuple2<String, Long>> joinedVerts = verticesById.join(validVertices);

            // mapear de vuelta a JavaRDD<scala.Tuple2<Object, String>> para Graph.apply
            JavaRDD<scala.Tuple2<Object, String>> remainingVerticesRDD = joinedVerts
                    .map(t -> new scala.Tuple2<Object, String>(t._1, t._2._1));

            // 4) Filtrar edges sin traer IDs al driver: join distribuido por src y luego por dst
            // edges keyed by src
            JavaPairRDD<Long, Edge<String>> edgesBySrc = current.edges().toJavaRDD()
                    .mapToPair(e -> new Tuple2<>(e.srcId(), e));

            // aseguramos que src esté en validVertices
            JavaPairRDD<Long, Tuple2<Edge<String>, Long>> edgesWithValidSrc = edgesBySrc.join(validVertices);

            // obtenemos edges que tienen src válido
            JavaRDD<Edge<String>> edgesAfterSrcFilter = edgesWithValidSrc
                    .map(t -> t._2._1); // t._2 = (Edge, degSrc) -> extraigo Edge

            // ahora key por dst y unimos con validVertices para asegurar dst válido
            JavaPairRDD<Long, Edge<String>> edgesByDst = edgesAfterSrcFilter
                    .mapToPair(e -> new Tuple2<>(e.dstId(), e));

            JavaPairRDD<Long, Tuple2<Edge<String>, Long>> edgesWithValidDst = edgesByDst.join(validVertices);

            // edges finales que tienen ambos endpoints en el core
            JavaRDD<Edge<String>> remainingEdgesRDD = edgesWithValidDst
                    .map(t -> t._2._1); // tomar el Edge

            // 5) reconstruir grafo con los vértices y edges remanentes
            current = Graph.apply(
                    JavaRDD.toRDD(remainingVerticesRDD),
                    JavaRDD.toRDD(remainingEdgesRDD),
                    "N/A", // default vertex attr
                    StorageLevel.MEMORY_ONLY(),
                    StorageLevel.MEMORY_ONLY(),
                    tagString,
                    tagString
            );

        }
    }


}
