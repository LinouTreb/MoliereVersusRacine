package moliereVSRacine;

import moliereVSRacine.Modelisation.DataMetrics;
import moliereVSRacine.Modelisation.FeatureExtraction;
import moliereVSRacine.Modelisation.ModelsTesting;
import moliereVSRacine.dataset.DatasetCreation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.mutable.WrappedArray;

import static moliereVSRacine.Modelisation.DataMetrics.sentencesCount;
import static moliereVSRacine.Modelisation.DataMetrics.wordsCount;


public class MainClass {

    public static void main( String [] args ){
        SparkConf conf = new SparkConf();
        conf.setMaster( "local[*]" );
        conf.setAppName( "My app" );
        JavaSparkContext sc = new JavaSparkContext( conf );
        sc.setLogLevel( "ERROR" );
        SparkSession spark = SparkSession
                .builder()
                .appName( "DatasetCreation" )
                .config( sc.getConf() )
                .getOrCreate()
                ;

        /* Creates the dataset from the text files contained in corpus folder*/
        DatasetCreation dc = new DatasetCreation( sc, spark );
        Dataset< Row > dataset = dc.getDataset().cache();

        /*Exploration of the data.*/
        DataMetrics dataMetrics = new DataMetrics();
        // Registration of User Defined Function
        spark.udf().register( "wordsCount", ( WrappedArray< String > s ) -> wordsCount( s ), DataTypes.IntegerType );
        spark.udf().register( "nbOfSentences", ( String s ) -> sentencesCount( s ), DataTypes.IntegerType );

        //adds new columns with sentences' number and words count
        Dataset< Row > regexTokenized = dataMetrics.setMetric( dataset );
        //regexTokenized.show();

        int [] settings  = new int[]{100,10};
        FeatureExtraction featureExtraction = new FeatureExtraction( );
        ;
        featureExtraction.set( settings, featureExtraction.dataPrep( regexTokenized, dataMetrics.getStopWords() ) );
        int i = 1;
        for(Dataset <Row> d : featureExtraction.getFeatureDatasets()){
            System.out.println("Test Feature extraction "+ i);
            ModelsTesting m = new ModelsTesting( d .cache());
            i++;
        }
   }
}

