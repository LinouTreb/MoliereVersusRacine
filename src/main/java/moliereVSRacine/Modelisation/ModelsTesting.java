package moliereVSRacine.Modelisation;


import moliereVSRacine.dataset.DatasetCreation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.*;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.mutable.WrappedArray;

import static moliereVSRacine.Modelisation.DataMetrics.sentencesCount;
import static moliereVSRacine.Modelisation.DataMetrics.wordsCount;


public class ModelsTesting
{
    private Dataset< Row > training;
    private Dataset< Row > test;
    /**
     *
     * @param dataset the {@link Dataset} containing the features
     */
    public ModelsTesting( Dataset< Row > dataset )
    {
        Dataset< Row >[] datasets = dataset.randomSplit( new double[]{ 0.7, 0.3 } , 50);
        this.training = datasets[ 0 ].persist(); // the training data
        this.test = datasets[ 1 ].persist(); //the test data
        launchModels();
    }

    /**
     * Launch 4 different models
     */
    private void launchModels()
    {
        /* Logistic regression */
        launchLogisticRegression();

        /* Decision Tree */
        launchDecisionTreeClassifier();

        /* Vector machine */
        launchLinearSVC();

        /* Random Forest */
        launchRandomForest();

        /* Naive Bayes */
        //launchNaiveBayes();
    }

    /**
     * Launch a Logistic regression model
     */
    private void launchLogisticRegression()
    {
        System.out.print( "Logistic regression : " );
        LogisticRegression lr = new LogisticRegression()
                .setMaxIter( 10 )
                .setRegParam( 0.3 )
                .setElasticNetParam( 0.8 );
        LogisticRegressionModel lrModel = lr.fit( training.persist() );
        lrModel.summary();
        Dataset< Row > predictions = lrModel.transform( this.test ).persist();
        evaluate( predictions );

    }


    /**
     * Launch a decision Tree Classifier model
     */
    private void launchDecisionTreeClassifier()
    {
        System.out.print("Desicion Tree classifier : ");
        DecisionTreeClassifier dt = new DecisionTreeClassifier()
                .setLabelCol( "label" )
                .setFeaturesCol( "features" );
        DecisionTreeClassificationModel dtModel = dt.fit( training );
        Dataset< Row > predictions = dtModel.transform( test );
        evaluate( predictions );
    }

    /**
     * Launch Linear SVC model
     */
    private void launchLinearSVC()
    {
        System.out.print("Desicion linear SVC : ");
        LinearSVC lsvc = new LinearSVC()
                .setMaxIter( 10 )
                .setRegParam( 0.1 );

        // Fit the model
        LinearSVCModel lsvcModel = lsvc.fit( training );

        Dataset< Row > predictions = lsvcModel.transform( test.persist() );
        evaluate( predictions );
    }

    private void launchRandomForest(){
        System.out.print("Random Forest : ");
        // Train a RandomForest model.
        RandomForestClassifier rf = new RandomForestClassifier()
                .setLabelCol("label")
                .setFeaturesCol("features");

        RandomForestClassificationModel rfModel = rf.fit(this.training);
        Dataset< Row > predictions = rfModel.transform( this.test );
        evaluate( predictions );
    }


    /**
     *
     * @param dataset the {@link Dataset} containing the predictions
     */
    private void evaluate( Dataset dataset )
    {
        BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator()
                .setLabelCol( "label" )
                .setRawPredictionCol( "prediction" )
                .setMetricName( "areaUnderROC" );
        double AUC = evaluator.evaluate( dataset.persist() );
        System.out.println( "AUC = " + ( AUC ) );
    }

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
        Dataset< Row > data = featureExtraction.dataPrep( regexTokenized, dataMetrics.getStopWords() );
        data.show();
        System.out.println("*******************************");
        featureExtraction.set( settings,  data);
        int i = 1;
        for(Dataset <Row> d : featureExtraction.getFeatureDatasets()){
            System.out.println("Test Feature extraction "+ i);
            ModelsTesting m = new ModelsTesting( d .cache());
            i++;
        }
    }


}



