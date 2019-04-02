package moliereVSRacine.Modelisation;

import moliereVSRacine.dataset.DatasetCreation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.mutable.WrappedArray;

import static moliereVSRacine.Modelisation.DataMetrics.sentencesCount;
import static moliereVSRacine.Modelisation.DataMetrics.wordsCount;

public class CrossValidation
{
    private Dataset< Row > training;
    private Dataset< Row > test;


    public void validation( Dataset< Row > dataset)
    {
        Dataset< Row >[] datasets = dataset.randomSplit( new double[]{ 0.7, 0.3 } );
        this.training = datasets[ 0 ].persist(); // the training data
        this.test = datasets[ 1 ].persist(); //the test data

        int vectorSize = 20;
        Word2Vec word2Vec = new Word2Vec()
                .setInputCol( "words" )
                .setOutputCol( "words2vec" )
                .setVectorSize( vectorSize )
                .setMinCount( 0 );
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols( new String[]{ "words2vec", "words_per_sentences" } )
                .setOutputCol( "features" );
        DecisionTreeClassifier dt = new DecisionTreeClassifier()
                .setLabelCol( "label" )
                .setFeaturesCol( "features" );
        Pipeline pipeline = new Pipeline()
                .setStages( new PipelineStage[]{ word2Vec, assembler, dt } );
        ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid( word2Vec.vectorSize(), new int[]{ 10, 30, 40 } )
                .build();

        CrossValidator cv = new CrossValidator()
                .setEstimator( pipeline )
                .setEvaluator( new BinaryClassificationEvaluator() )
                .setEstimatorParamMaps( paramGrid )
                .setNumFolds( 3 )  // Use 3+ in practice
                .setParallelism( 1 );  // Evaluate up to 2 parameter settings in parallel
        CrossValidatorModel cvModel = cv.fit( this.training.persist() );
        Dataset< Row > predictions = cvModel.transform( this.test );

        for ( Row r : predictions.select( "label", "text", "probability", "prediction" ).collectAsList() ) {
            System.out.println( "(" + r.get( 0 ) + ", " + r.get( 1 ) + ") --> prob=" + r.get( 2 )
                    + ", prediction=" + r.get( 3 ) );

        }

    }



    public static void main (String [] args){
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


        FeatureExtraction featureExtraction = new FeatureExtraction( );
        Dataset< Row > data = featureExtraction.dataPrep( regexTokenized, dataMetrics.getStopWords() );
        CrossValidation crossValidation = new CrossValidation();
        crossValidation.validation( data );

        int [] settings  = new int[]{100,10};
    }
}
