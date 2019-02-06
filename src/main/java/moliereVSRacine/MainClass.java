package moliereVSRacine;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;

public class MainClass {

    public static void main( String [] args ){
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("My app");

        JavaSparkContext sc = new JavaSparkContext(conf);
        //sc.setLogLevel("WARN");
        JavaRDD < String > data = getData(sc);
        System.out.println( data.take(5));
        System.out.println( "**********************" );
        System.out.println(data.take ( 1 ));
    }

    public static JavaRDD< String > getData(JavaSparkContext sc)
    {

        String URLracine1 = "C:\\Users\\ctreb\\Documents\\Cnam\\RCP216\\Esther.txt";
        //File file = new File(URLracine1);
        return ( sc.textFile( URLracine1 ) );

    }
}

