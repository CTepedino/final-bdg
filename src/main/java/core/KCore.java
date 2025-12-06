package core;

import exception.IllegalProgramArgumentException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

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



        session.close();
    }
}