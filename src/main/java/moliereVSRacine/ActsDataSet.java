package moliereVSRacine;

import moliereVSRacine.Utilitaries.FileToPlay;
import moliereVSRacine.Utilitaries.Play;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class ActsDataSet {

    private Dataset< Row> fullDataset;

    private static String m1 = "C:\\Users\\ctreb\\IdeaProjects\\MoliereVersusRacine\\src\\main\\resources\\andromaque.txt";
    private static String m2 = "C:\\Users\\ctreb\\IdeaProjects\\MoliereVersusRacine\\src\\main\\resources\\athalie.txt";
    private  static String m3 = "C:\\Users\\ctreb\\IdeaProjects\\MoliereVersusRacine\\src\\main\\resources\\avare.txt";
    private static String m4 = "C:\\Users\\ctreb\\IdeaProjects\\MoliereVersusRacine\\src\\main\\resources\\britannicus.txt";
    private static String m5 = "C:\\Users\\ctreb\\IdeaProjects\\MoliereVersusRacine\\src\\main\\resources\\domjuan.txt";
    private static String m6 = "C:\\Users\\ctreb\\IdeaProjects\\MoliereVersusRacine\\src\\main\\resources\\ecole.txt";
    private static String m7 = "C:\\Users\\ctreb\\IdeaProjects\\MoliereVersusRacine\\src\\main\\resources\\iphrigenie.txt";
    private static String m8 = "C:\\Users\\ctreb\\IdeaProjects\\MoliereVersusRacine\\src\\main\\resources\\scapin.txt";
    private static String m9 = "C:\\Users\\ctreb\\IdeaProjects\\MoliereVersusRacine\\src\\main\\resources\\phedre.txt";
    private static String m10 = "C:\\Users\\ctreb\\IdeaProjects\\MoliereVersusRacine\\src\\main\\resources\\tartuffe.txt";





    public ActsDataSet(ArrayList<String> plays, JavaSparkContext sc,SparkSession spark){
//        ArrayList <String > data = new ArrayList<>(10);
//        for ( Play play : plays ){
//            data.addAll(play.getActs());
//        }
        JavaRDD dataRDD = arrayListToRDD(plays, sc);
        fullDataset =  RDDToDataset(dataRDD, spark);
    }

    public JavaRDD<String> arrayListToRDD(ArrayList<String> acts, JavaSparkContext sc ){
       JavaRDD<String > actsRDD = sc.parallelize(acts);
        return (actsRDD);
    }

    public Dataset<Row> RDDToDataset(JavaRDD< String> actsRDD, SparkSession spark){

        String chainSchema = "Text Author";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : chainSchema.split( " ") )
        {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add( field );
        }
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD (text) to Rows
        JavaRDD<Row> rowRDD = actsRDD.map((Function<String, Row>) record -> {
            String[] attributes = record.split("#");
            return RowFactory.create(attributes[0].trim(), attributes[1].trim() );

        });

// Apply the schema to the RDD
        Dataset<Row> partialDataset = spark.createDataFrame(rowRDD, schema);
        partialDataset.show(30);
        return partialDataset;
    }

    public Dataset<Row> getFullDataset() {
        return fullDataset;
    }

    public static void main (String [] args){
        ArrayList<String> plays = new ArrayList<>(10);
        plays.addAll(new FileToPlay(m1).getData());
        plays.addAll(new FileToPlay(m2).getData());
        plays.addAll(new FileToPlay(m3).getData());
        plays.addAll(new FileToPlay(m4).getData());
        plays.addAll(new FileToPlay(m5).getData());
        plays.addAll(new FileToPlay(m6).getData());
        plays.addAll(new FileToPlay(m7).getData());
        plays.addAll(new FileToPlay(m8).getData());
        plays.addAll(new FileToPlay(m9).getData());
        plays.addAll(new FileToPlay(m10).getData());

        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("My app");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("DatasetCreation")
                .config(sc.getConf())
                .getOrCreate();
        ActsDataSet dataSet = new ActsDataSet(plays, sc, sparkSession);
        System.out.println("data size : "+ plays.size());
        dataSet.getFullDataset().show();
    }
}
