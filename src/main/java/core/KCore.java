package core;

import exception.IllegalProgramArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

public class KCore {
    //TODO -> los archivos son directorios
    //TODO -> connectedComponents fix

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

        return current; //TODO -> fix largest
    }
//
//    public static Graph<String, String> largestConnectedComponent(Graph<String, String> graph, JavaSparkContext jsc) {
//
//        // 1️⃣ Calcular componentes conectados
//        Graph<Object, String> ccGraph = graph.ops().connectedComponents();
//
//        // 2️⃣ Contar tamaño de cada componente
//        JavaPairRDD<Object, Long> componentCounts = ccGraph.vertices().toJavaRDD()
//                .mapToPair(v -> new Tuple2<>(v._2(), 1L))
//                .reduceByKey(Long::sum);
//
//        // 3️⃣ Componente más grande
//        Tuple2<Object, Long> largest = componentCounts.max(Comparator.comparingLong(Tuple2::_2));
//        final Object mainComponentId = largest._1(); // final para lambdas
//
//        // 4️⃣ Filtrar vertices del componente más grande
//        JavaPairRDD<Object, String> originalVertices = graph.vertices().toJavaRDD()
//                .mapToPair(v -> new Tuple2<>(v._1(), v._2()));
//
//        JavaPairRDD<Object, Object> componentVertices = ccGraph.vertices().toJavaRDD()
//                .mapToPair(v -> new Tuple2<>(v._1(), v._2()))
//                .filter(v -> v._2().equals(mainComponentId));
//
//        JavaPairRDD<Object, String> filteredVertices = componentVertices.join(originalVertices)
//                .mapToPair(v -> new Tuple2<>(v._1(), v._2()._2()));
//
//        // 5️⃣ Filtrar aristas completamente dentro del componente usando join con vertices
//        JavaPairRDD<Object, Object> vertexIdsRDD = filteredVertices.mapToPair(v -> new Tuple2<>(v._1(), null));
//
//        JavaRDD<Edge<String>> filteredEdges = graph.edges().toJavaRDD()
//                .mapToPair(e -> new Tuple2<>(e.srcId(), e))
//                .join(vertexIdsRDD) // Filtrar srcId
//                .map(Tuple2::_2)
//                .map(Tuple2::_1)
//                .mapToPair(e -> new Tuple2<>(e.dstId(), e))
//                .join(vertexIdsRDD) // Filtrar dstId
//                .map(Tuple2::_2)
//                .map(Tuple2::_1);
//
//        // 6️⃣ Construir el subgrafo
//        return Graph.apply(
//                JavaRDD.toRDD(filteredVertices),
//                JavaRDD.toRDD(filteredEdges),
//                "default",
//                StorageLevel.MEMORY_ONLY(),
//                StorageLevel.MEMORY_ONLY(),
//                STRING_TAG,
//                STRING_TAG
//        );
//    }

    public static void saveToHDFS(Graph<String, String> graph, String timestamp, JavaSparkContext jsc) {
        try {
            Configuration conf = jsc.hadoopConfiguration();
            FileSystem fs = FileSystem.get(new URI("hdfs:///"), conf);
            Path homePath = fs.getHomeDirectory();

            // Vertices
            JavaRDD<String> verticesRDD = graph.vertices().toJavaRDD()
                    .map(v -> v._1 + "," + v._2);

            Path verticesTmpPath = new Path(homePath, timestamp + "-nodes-tmp");
            Path verticesFinalPath = new Path(homePath, timestamp + "-nodes.csv");

            // Borrar si existe
            if (fs.exists(verticesTmpPath)) fs.delete(verticesTmpPath, true);
            if (fs.exists(verticesFinalPath)) fs.delete(verticesFinalPath, true);

            // Guardar RDD temporal
            verticesRDD.coalesce(1).saveAsTextFile(verticesTmpPath.toUri().toString());

            // Renombrar part-00000 a CSV final
            FileStatus[] vertexFiles = fs.listStatus(verticesTmpPath);
            for (FileStatus file : vertexFiles) {
                if (file.getPath().getName().startsWith("part-")) {
                    fs.rename(file.getPath(), verticesFinalPath);
                    break;
                }
            }
            fs.delete(verticesTmpPath, true);

            // Edges
            JavaRDD<String> edgesRDD = graph.edges().toJavaRDD()
                    .map(e -> e.srcId() + "," + e.dstId());

            Path edgesTmpPath = new Path(homePath, timestamp + "-edges-tmp");
            Path edgesFinalPath = new Path(homePath, timestamp + "-edges.csv");

            if (fs.exists(edgesTmpPath)) fs.delete(edgesTmpPath, true);
            if (fs.exists(edgesFinalPath)) fs.delete(edgesFinalPath, true);

            edgesRDD.coalesce(1).saveAsTextFile(edgesTmpPath.toUri().toString());

            FileStatus[] edgeFiles = fs.listStatus(edgesTmpPath);
            for (FileStatus file : edgeFiles) {
                if (file.getPath().getName().startsWith("part-")) {
                    fs.rename(file.getPath(), edgesFinalPath);
                    break;
                }
            }
            fs.delete(edgesTmpPath, true);

        } catch (Exception e) {
            throw new RuntimeException("Error saving graph in HDFS: " + e.getMessage(), e);
        }
    }

}
