package moliereVSRacine.Modelisation;

import moliereVSRacine.dataset.DatasetCreation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.util.MLWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.mutable.WrappedArray;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import static moliereVSRacine.Modelisation.DataMetrics.sentencesCount;
import static moliereVSRacine.Modelisation.DataMetrics.wordsCount;

public class CrossValidation implements Serializable, scala.Serializable {
    private Dataset< Row > training;
    private Dataset< Row > test;


    public void validation( Dataset< Row > dataset)
    {
        int vectorSize = 20;

        Word2Vec word2Vec = new Word2Vec()
                .setInputCol("filtered")
                .setOutputCol("words2vec")
                .setVectorSize(vectorSize)
                .setMinCount(0);
        Word2VecModel model = word2Vec.fit(dataset.persist());
        Dataset<Row> w2v = model.transform(dataset);

        /* Assembles several features in one vector*/
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String []{"words2vec", "words_per_sentences"})
                .setOutputCol("features");
        Dataset<Row> assembled =  assembler.transform( w2v ).persist();

        Dataset< Row >[] datasets = assembled.randomSplit( new double[]{ 0.7, 0.3 } , 50);
        Dataset< Row > training = datasets[ 0 ].persist(); // the training data
        Dataset< Row > test = datasets[ 1 ].persist(); //the test data
        DecisionTreeClassifier dt = new DecisionTreeClassifier()
                .setLabelCol( "label" )
                .setFeaturesCol( "features" );
        DecisionTreeClassificationModel dtModel = dt.fit( training );

        MLWriter mlWriter = dtModel.write();
        //Dataset< Row > predictions = dtModel.transform( test );


        PipelineStage [] pipelineStage = { word2Vec,assembler, dt};
        Pipeline pipeline = new Pipeline()
                .setStages( pipelineStage );


        PipelineModel pipelineModel = pipeline.fit(dataset.cache());

        try {
            mlWriter.overwrite().save(CrossValidation.class.getClassLoader().getResource("pipeline2.txt").getPath());
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("oulalal");
        }

        ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid( word2Vec.vectorSize(), new int[]{ 10, 30, 40 } )
                .build();

        //PipelineModel pipelineModel = pipeline.fit(dataset.cache());




    }



    public static void main (String [] args){
        SparkConf conf = new SparkConf();

        conf.setMaster( "local[*]" );
        conf.setAppName( "My app" );
        JavaSparkContext sc = new JavaSparkContext( conf );
        sc.setLogLevel( "ERROR" );
        SparkSession spark = SparkSession
                .builder()
                .appName( "CrossValidation" )
                .config( sc.getConf() )
                .getOrCreate()
                ;





        /* Creates the dataset from the text files contained in corpus folder*/
        DatasetCreation dc = new DatasetCreation( sc, spark );
        Dataset< Row > dataset = dc.getDataset().cache();

        /*Exploration of the data.*/
        DataMetrics dataMetrics = new DataMetrics();
        // Registration of User Defined Function
        spark.udf().register( "wordsCount", ( WrappedArray< String > s ) ->             wordsCount( s ), DataTypes.IntegerType );
        spark.udf().register( "nbOfSentences", ( String s ) -> sentencesCount( s ), DataTypes.IntegerType );

        //adds new columns with sentences' number and words count
        Dataset< Row > regexTokenized = dataMetrics.setMetric( dataset.persist() );
        //regexTokenized.show();


        FeatureExtraction featureExtraction = new FeatureExtraction( );
        Dataset< Row > data = featureExtraction.dataPrep( regexTokenized.persist(), dataMetrics.getStopWords() ).persist();
        data.show();


        CrossValidation crossValidation = new CrossValidation();
        crossValidation.validation(data);
//        int vectorSize = 20;
//
//        Word2Vec word2Vec = new Word2Vec()
//                .setInputCol( "filtered" )
//                .setOutputCol( "words2vec" )
//                .setVectorSize( 20 )
//                .setMinCount( 0 );
//        Word2VecModel model = word2Vec.fit(training.persist());
//        Dataset<Row> result = model.transform(training);
//
//        VectorAssembler assembler = new VectorAssembler()
//                .setInputCols( new String[]{ "words2vec", "words_per_sentences" } )
//                .setOutputCol( "features" )
//                ;
//        DecisionTreeClassifier dt = new DecisionTreeClassifier()
//                .setLabelCol( "label" )
//                .setFeaturesCol( "words2vec" );
//
//        Pipeline pipeline = new Pipeline()
//                .setStages( new PipelineStage[]{  word2Vec , dt} );
//        PipelineModel p = new Pipeline()
//                .setStages( new PipelineStage[]{  assembler , dt})
//                .fit(result.cache());

                      // PipelineModel pipelineModel = pipeline.fit(training.cache());
//        Dataset< Row > predic = pipelineModel.transform(training);
//        predic.show();
//        ParamMap[] paramGrid = new ParamGridBuilder()
//                .addGrid( word2Vec.vectorSize(), new int[]{ 10 } )
//               // .addGrid(lr.regParam(), new double[] {0.1, 0.01})
//                .build();
//
//        CrossValidator cv = new CrossValidator()
//                .setEstimator(  pipeline)
//                .setEvaluator( new BinaryClassificationEvaluator() )
//                .setEstimatorParamMaps( paramGrid )
//                .setNumFolds( 1 )  // Use 3+ in practice
//                .setParallelism( 1 )
//                ;  // Evaluate up to 2 parameter settings in parallel
//        CrossValidatorModel cvModel = cv.fit(result.persist() );
//        Dataset< Row > predictions = cvModel.transform( test );
//
//        for ( Row r : predictions.select( "label", "text", "probability", "prediction" ).collectAsList() ) {
//            System.out.println( "(" + r.get( 0 ) + ", " + r.get( 1 ) + ") --> prob=" + r.get( 2 )
//                    + ", prediction=" + r.get( 3 ) );

      //  }

    }



}
