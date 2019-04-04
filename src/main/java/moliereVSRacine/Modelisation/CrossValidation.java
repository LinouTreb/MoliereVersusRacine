package moliereVSRacine.Modelisation;

import moliereVSRacine.dataset.DatasetCreation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.linalg.DenseVector;
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
import java.util.ArrayList;

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

        LogisticRegression lr = new LogisticRegression()
                .setMaxIter( 10 )
                .setRegParam( 0.3 )
                .setElasticNetParam( 0.8 );

        Pipeline pipeline = new Pipeline().setStages( new PipelineStage[]{  stringIndexer, remover,hashingTF, idf ,assembler, lr} );

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

        try {
            ( cvModel)
                    .save("C:\\Users\\Dani-Linou\\IdeaProjects\\MoliereVersusRacine\\src\\main\\resources\\model");
        } catch (IOException e) {
            e.printStackTrace();
        }
        Dataset<Row> result = cvModel.transform(dataset);
        result.show();
    }

    public void visualize(Dataset<Row> result){
        PCAModel pca = new PCA()
                .setInputCol("features")
                .setOutputCol("pcaFeatures")
                .setK(2)/* Not possible due to memory issue*/
                .fit(result);
        System.out.println(pca.explainParams());

        Dataset<Row> pcaFeatures = pca.transform(result).select("pcaFeatures");

        result.show(false);

        ArrayList<Double> a = new ArrayList<>();
        ArrayList<Double> b = new ArrayList<>();

        for (int i = 0; i<= pcaFeatures.toJavaRDD().collect().size()-1; i++){
            DenseVector denseVector =  (DenseVector )result.toJavaRDD().collect().get(i).get(0);

            a.add(denseVector.apply(0));
            b.add(denseVector.apply(1));
        }
        System.out.println(a);
        System.out.println(b);

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


        Dataset< Row > regexTokenized = dataMetrics.setMetric2( dataset.persist() );
        //regexTokenized.show();

        CrossValidation crossValidation = new CrossValidation();
        crossValidation.featureExtraction(regexTokenized, dataMetrics.getStopWords());
    }



}
