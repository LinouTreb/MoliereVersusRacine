package moliereVSRacine.Modelisation;

import moliereVSRacine.dataset.DatasetCreation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.*;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.mutable.WrappedArray;

import java.io.IOException;
import java.io.Serializable;

import static moliereVSRacine.Modelisation.DataMetrics.sentencesCount;
import static moliereVSRacine.Modelisation.DataMetrics.wordsCount;

public class CrossValidation implements Serializable, scala.Serializable {



    private void featureExtraction(Dataset<Row> dataset, String[] stopWords)
    {


        StringIndexer stringIndexer = new StringIndexer().
                setInputCol( "author" ).
                setOutputCol( "label" );

        StopWordsRemover remover = new StopWordsRemover()
                .setInputCol("words")
                .setOutputCol("filtered")
                .setStopWords( stopWords );

        HashingTF hashingTF = new HashingTF()
                .setNumFeatures(10000)
                .setInputCol("filtered")
                .setOutputCol("rawFeatures");

        IDF idf = new IDF().
                setInputCol(hashingTF.getOutputCol()).
                setOutputCol("TFfeatures");

        String [] colNames = {idf.getOutputCol(), "words_per_sentences"};
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(colNames)
                .setOutputCol("features");


//        Word2Vec word2Vec = new Word2Vec()
//                .setInputCol("filtered")
//                .setOutputCol("words2vec")
//                .setVectorSize(100)
//                .setMinCount(2);

//        VectorAssembler assembler = new VectorAssembler()
//                .setInputCols(new String []{"words2vec", "words_per_sentences"})
//                .setOutputCol("features");

        LogisticRegression lr = new LogisticRegression()
                .setMaxIter( 10 )
                .setRegParam( 0.3 )
                .setElasticNetParam( 0.8 );

        //Pipeline pipeline = new Pipeline().setStages( new PipelineStage[]{  stringIndexer, remover, word2Vec,assembler, lr} );

        Pipeline pipeline = new Pipeline().setStages( new PipelineStage[]{  stringIndexer, remover,hashingTF, idf ,assembler, lr} );

//        ParamMap[] paramGrid = new ParamGridBuilder()
//                .addGrid( word2Vec.vectorSize(), new int[]{ 100, 200, 300 } )
//                .addGrid(lr.regParam(), new double[] {0.001, 0.01, 0.1})
//                .build();

        ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid( hashingTF.numFeatures(), new int[]{ 5000, 10000, 15000 } )
                .addGrid(lr.regParam(), new double[] {0.001, 0.01, 0.1})
                .build();

        CrossValidator cv = new CrossValidator()
                .setEstimator(  pipeline )
                .setEvaluator( new BinaryClassificationEvaluator() )
                .setEstimatorParamMaps( paramGrid )
                .setNumFolds( 2 )  // Use 3+ in practice

                ;  // Evaluate up to 2 parameter settings in parallel
        CrossValidatorModel cvModel = cv.fit(dataset );


        Pipeline bestModel = (Pipeline) cvModel.bestModel().parent();
        Param vs =  bestModel.getStages()[2].getParam("numFeatures");
        System.out.println("value of numfeatures : " +bestModel.getStages()[2].get(vs));
        Param rp = bestModel.getStages()[5].getParam("regParam");
        System.out.println("value of regParam : " +bestModel.getStages()[5].get(rp));

//        Param vs =  bestModel.getStages()[2].getParam("vectorSize");
//        System.out.println("value of vectorSize : " +bestModel.getStages()[2].get(vs));
//        Param rp = bestModel.getStages()[4].getParam("regParam");
//        System.out.println("value of regParam : " +bestModel.getStages()[4].get(rp));


        try {
            ( cvModel)
                    .save("C:\\Users\\Dani-Linou\\IdeaProjects\\MoliereVersusRacine\\src\\main\\resources\\model4");
        } catch (IOException e) {
            e.printStackTrace();
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
        Dataset< Row > regexTokenized = dataMetrics.setMetric2( dataset.persist() );
        //regexTokenized.show();





        CrossValidation crossValidation = new CrossValidation();
        crossValidation.featureExtraction(regexTokenized, dataMetrics.getStopWords());


    }



}
