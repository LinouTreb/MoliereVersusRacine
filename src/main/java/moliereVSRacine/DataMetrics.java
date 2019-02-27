package moliereVSRacine;

import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import processing.core.PImage;
import scala.collection.mutable.WrappedArray;

import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.apache.spark.sql.functions.*;


public class DataMetrics
{

    //private Dataset< Row > dataset;
    private JavaSparkContext sc;
    private SparkSession spark;

    public DataMetrics()
    {
    }

    /**
     * @param dataset, the dataset to process.
     * @param colName, the name of the column containing the different classes/categories.
     * @return the number of classes/categories.
     */
    public int classCount( Dataset< Row > dataset, String colName )
    {
        return ( int ) dataset.select( colName ).dropDuplicates().count();
    }

    /**
     * Counts the number of classes in all samples.
     *
     * @param dataset,   the dataset to process.
     * @param colName,   the name of the column containing the different classes/categories.
     * @param className, the name of the class of interest.
     * @return
     */
    public int samplesClassCount( Dataset< Row > dataset, String colName, String className )
    {

        return ( int ) dataset.filter( col( colName ).like( className ) ).count();

    }

    /**
     * Counts the number of items (words here) in a WrappedArray (of Strings).
     *
     * @param text, the Wrappedarray provided by RegexTokenizer.
     * @return the number of items.
     */
    public int wordsCount( WrappedArray< String > text )
    {
        return text.size();
    }


    /**
     * For a given dataset and its column containing Strings, return the same dataset with in additiona column
     * containing the number of words of the input column
     *
     * @param dataset, the dataset to process
     * @param colName, the name of the column containing the text to process
     * @return the dataset plus a column containing the number of words in the input column.
     */
    public Dataset< Row > numberOfwords( Dataset< Row > dataset, String colName )
    {
        RegexTokenizer regexTokenizer = new RegexTokenizer()
                .setInputCol( colName )
                .setOutputCol( "Words" )
                .setPattern( "\\W" );  // alternatively .setPattern("\\w+").setGaps(false);
        Dataset< Row > regexTokenized = regexTokenizer.transform( dataset );
        spark.udf().register( "wordsCount", ( WrappedArray s ) -> wordsCount( s ), DataTypes.IntegerType );
        Dataset< Row > c = regexTokenized
                .withColumn( "nb_Words", callUDF( "wordsCount", col( "Words" ) ) );

        return c;
    }

    public Dataset< Row > wordsFrequency (Dataset< Row > dataset, String colName ){
        Dataset <Row > allWords = dataset.groupBy().agg(collect_set( colName ).as( "AllWords"));
        allWords.show(false);
        allWords = allWords.withColumn( "AllWordsWords", concat_ws( " ",col("AllWords" ) ));
        allWords.show(false);
        allWords = allWords.unionAll( allWords );
        RegexTokenizer regexTokenizer = new RegexTokenizer()
                .setInputCol( "AllWordsWords" )
                .setOutputCol( "SplitWords" )
                .setPattern( "\\W" );  // alternatively .setPattern("\\w+").setGaps(false);
        Dataset< Row > regexTokenized = regexTokenizer.transform( allWords );
        Dataset<Row> splitWords = regexTokenized.select("SplitWords");
        regexTokenized.show();
        //regexTokenized.withColumn( "SplitWords", concat_ws( " ",col("SplitWords" ) ));
        for(int i = 0; i<= splitWords.dtypes().length-1; i++){
            System.out.println(splitWords.dtypes()[i]);
            //(text,ArrayType(StringType,true))
            //(text,ArrayType(StringType,true))
        }
//        CountVectorizerModel cvModel = new CountVectorizer()
//                .setInputCol( "SplitWords" )
//                .setOutputCol( "feature" )
//                .setVocabSize( 3 )
//                .setMinDF( 2 )
//                .fit( splitWords );
//        cvModel.transform(splitWords).show(false);
        //return cvModel.transform(splitWords);
        return splitWords;
    }

    /**
     * @param dataset the dataset
     * @param colName the name of the column from which you want the median
     * @return the median
     */
    public double getMedianNumber( Dataset< Row > dataset, String colName )
    {
        DataFrameStatFunctions dataFrameStatFunctions = new DataFrameStatFunctions( dataset );
        double[] wordsCountArray = dataFrameStatFunctions.approxQuantile( colName, new double[]{ 0.5, 0.2, 0.1, 1 }, 0 );
        return wordsCountArray[ 0 ];
    }


    /**
     * Breaks a text into its sentences and return its number.
     *
     * @param text, a String.
     * @return the number of words in the string separated by a space (//W).
     */
    public int sentencesCount( String text )
    {
        ArrayList< String > count;
        if ( text != null && !( text.equals( "" ) ) ) {
            count = new ArrayList<>();
            BreakIterator bi = BreakIterator.getSentenceInstance( Locale.FRENCH );
            bi.setText( text );
            int start = 0;
            int end;// = 0;
            while ( ( end = bi.next() ) != BreakIterator.DONE ) {
                count.add( text.substring( start, end ) );
                start = end;
            }
            return count.size();
        } else {
            return ( 0 );
        }
    }

    /**
     * Applies a function (sentencesCount) which calculate the number of sentences in each row of the column "Text"
     *
     * @param dataset, the dataframe containing all the data
     * @return the dataset plus the number of sentences
     */
    public Dataset< Row > numberOfSentences( Dataset< Row > dataset, String colName )
    {
        spark.udf().register( "nbOfSentences", ( String s ) -> sentencesCount( s ), DataTypes.IntegerType );
        Dataset< Row > c = dataset
                .withColumn( "nb_sentences", callUDF( "nbOfSentences", col( colName ) ) );
        return c;
    }

    /**
     * Print the dataset metrics (may be into file ???)
     *
     * @param dataset, the dataset from which you want the metrics
     */
    public void exportMetric( Dataset dataset )
    {
        ArrayList< String > metricsList = new ArrayList( 4 );
        metricsList.add( "nb of classes : " +
                Integer.toString( this.classCount( dataset, "Author" ) ) );
        metricsList.add( "nb of data for classes moliere : " +
                Integer.toString( this.samplesClassCount( dataset, "Author", "moliere" ) ) );
        metricsList.add( "nb of data for classes racine : " +
                Integer.toString( this.samplesClassCount( dataset, "Author", "racine" ) ) );
        metricsList.add( "Number of words per sample (median) : " +
                Double.toString( this.getMedianNumber( dataset, "nb_Words" ) ) );

        for ( String s : metricsList ) {
            System.out.println( s );
        }
    }

    /**
     * Function to add two columns : first the number of words per sample and the numbers of sentences per sample
     *
     * @param dataset the dataset
     * @return the upgraded dataset
     */
    public Dataset< Row > setMetric( Dataset< Row > dataset )
    {

        dataset = this.numberOfwords( dataset, "Text" );
        dataset.show();
        dataset = this.numberOfSentences( dataset, "Text" );
        dataset.show();
        return dataset;
    }


    public void wordFrequency()
    {

    }


    public void setSc( JavaSparkContext sc )
    {
        this.sc = sc;
    }

    public void setSpark( SparkSession spark )
    {
        this.spark = spark;
    }


    public static void main( String[] args )
    {
        SparkConf conf = new SparkConf();
        conf.setMaster( "local[*]" );
        conf.setAppName( "My app" );
        JavaSparkContext sc = new JavaSparkContext( conf );
        sc.setLogLevel( "WARN" );
        SparkSession spark = SparkSession
                .builder()
                .appName( "DatasetCreation" )
                .config( sc.getConf() )
                .getOrCreate();
        //sc.setLogLevel( "WARN" );

        List< Row > data = Arrays.asList(
                RowFactory.create(
                        "Les moments me sont chers, écoutez-moi, Thésée.\n" +
                                "Rend au jour, qu'ils souillaient, toute sa pureté.", "moliere" ),
                RowFactory.create( "I wish Java could use case classes", "racine" ),
                RowFactory.create( "Logistic,regression,models,are,neat", "moliere" )
        );

        StructType schema = new StructType( new StructField[]{
                new StructField( "Text", DataTypes.StringType, false, Metadata.empty() ),
                new StructField( "Author", DataTypes.StringType, false, Metadata.empty() )
        } );

        Dataset< Row > sentenceDataFrame = spark.createDataFrame( data, schema );
        DataMetrics dm = new DataMetrics();

        dm.setSc( sc );
        dm.setSpark( spark );
//        Dataset< Row > s = dm.numberOfSentences( sentenceDataFrame, "Text" );
//        s.show();
//        Dataset< Row > v = dm.numberOfwords( s, "Text" );
//        v.show();
        sentenceDataFrame = dm.setMetric( sentenceDataFrame );
        sentenceDataFrame.show();
        dm.exportMetric( sentenceDataFrame );
        sentenceDataFrame.show();
        System.out.println( sentenceDataFrame.select( "nb_words" ).collect().getClass() );
        Row[] nbWords = ( Row[] ) sentenceDataFrame.select( "nb_words" ).collect();
        System.out.println( "row size : " + nbWords.length );
        int[] values = new int[ nbWords.length ];
        for ( int i = 0; i <= values.length - 1; i++ ) {
            System.out.println( "i : " + i );
            values[ i ] = ( int ) nbWords[ i ].apply( 0 );
            System.out.println( values[ i ] );
        }

//        Pdisplay p = new Pdisplay();
//        p.displayHistogramm( values );


        List< Row > data1 = Arrays.asList(
                RowFactory.create( Arrays.asList( "a", "b", "c" ) ),
                RowFactory.create( Arrays.asList( "a", "b", "b", "c", "a" ) )
        );
        StructType schema1 = new StructType( new StructField[]{
                new StructField( "text", new ArrayType( DataTypes.StringType, true ), false, Metadata.empty() )
        } );
        Dataset< Row > df = spark.createDataFrame( data1, schema1 );
        for(int i = 0; i<= df.dtypes().length-1; i++){
            System.out.println(df.dtypes()[i]);
        }

        //Dataset< Row > df2 =
                //sentenceDataFrame.agg("Words", "words")
        // fit a CountVectorizerModel from the corpus
//        CountVectorizerModel cvModel = new CountVectorizer()
//                .setInputCol( "Words" )
//                .setOutputCol( "feature" )
//                .setVocabSize( 10 )
//                .setMinDF( 0 )
//                .fit( sentenceDataFrame );

        // alternatively, define CountVectorizerModel with a-priori vocabulary
//        CountVectorizerModel cvm = new CountVectorizerModel( new String[]{ "a", "b", "c" } )
//                .setInputCol( "text" )
//                .setOutputCol( "feature" );

        CountVectorizerModel cvModel2 = new CountVectorizer()
                .setInputCol( "text" )
                .setOutputCol( "feature" )
                .setVocabSize( 10 )
                .setMinDF( 0 )
                .fit( df );
//        cvModel.transform(sentenceDataFrame).show(false);
        cvModel2.transform(df).show(false);
//        Dataset <Row > allWords = sentenceDataFrame.groupBy().agg(collect_set( "Words" ).as( "AllWords"));
//       allWords.select("AllWords").foreach( ( ForeachFunction< Row> ) row -> row.get(0) );
//        allWords.show();
        Dataset< Row > df1 = dm.wordsFrequency( sentenceDataFrame, "Text" );
        df.show();
        df1.show(false);
        CountVectorizerModel cvModel3 = new CountVectorizer()
                .setInputCol( "SplitWords" )
                .setOutputCol( "feature" )
                .setVocabSize( 2 )
                .setMinDF( 0 )


                .fit( df1 );
        cvModel3.transform(df1).show(false);


    }
}




//TODO Distribution of sample length: Distribution showing the number of words per sample in the dataset.
//TODO Frequency distribution of words: Distribution showing the frequency (number of occurrences) of each word in the dataset.