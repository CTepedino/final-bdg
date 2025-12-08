package core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class HDFSUtils {

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

    private static void movePartToTarget(FileSystem fs, Path tmpDir, Path target) throws IOException {
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
