package moliereVSRacine.Modelisation;

import moliereVSRacine.processing.Visualization;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import processing.core.PApplet;
import processing.data.IntDict;
import scala.Tuple2;
import scala.collection.mutable.WrappedArray;

import java.io.Serializable;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;

import static org.apache.spark.sql.functions.*;


/**
 * Static methods to explore the dataset.
 */
public class DataMetrics implements Serializable
{

    private static IntDict intDict;
    private static int total;
    private int numberOfWordsClasse0;
    private int numberOfWordsClasse1;
    private String[] stopWords;
    private String[] processingArgs = { "Visualization" };
    //private double


    public static IntDict getIntDict()
    {
        return intDict;
    }

    private static void setIntDict( IntDict intDict )
    {
        DataMetrics.intDict = intDict;
    }

    public static int getTotal()
    {
        return total;
    }

    private static void setTotal( int total )
    {
        DataMetrics.total = total;
    }



    /**
     * Counts the number of items (words here) in a WrappedArray (of Strings).
     *
     * @param text, the Wrapped array provided by RegexTokenizer.
     * @return the number of items.
     */
    public static int wordsCount( WrappedArray< String > text )
    {
        return text.size();
    }

    /**
     * For a given dataset and its column containing Strings, return the same dataset with in addition a column
     * containing the number of words of the input column
     *
     * @param dataset, the dataset to process
     * @return the dataset plus a column containing the number of words in the input column.
     */
    private Dataset< Row > numberOfWords( Dataset< Row > dataset )
    {

        RegexTokenizer regexTokenizer = new RegexTokenizer()
                .setInputCol( "text" )
                .setOutputCol( "words" )
                .setPattern( "\\W" );  // alternatively .setPattern("\\w+").setGaps(false);
        Dataset< Row > regexTokenized = regexTokenizer.transform( dataset );
        Column c1 = callUDF( "wordsCount", col( "words" ) );
        return regexTokenized
                .withColumn( "nb_Words", c1 );
    }


    /**
     * @param dataset - the dataset
     * @return a {@link JavaPairRDD} containing words and its occurences
     */
    private JavaPairRDD< String, Integer > wordsFrequency3( Dataset< Row > dataset )
    {
        /*Unwraps the "words" column*/
        Dataset< Row > concatAllWords = dataset
                .withColumn( "allTheWords", concat_ws( " ", col( "words" ) ) );
        /*Converts the dataset into javaRDD*/
        JavaRDD< Row > javaRDD = concatAllWords.persist().select( "allTheWords" ).toJavaRDD();
        /* Counts the all the words occurrences */
        JavaRDD< String > stringJavaRDD = javaRDD.map( ( Function< Row, String > ) ( row ->
                ( String ) row.get( 0 ) ) );
        JavaRDD< String > linesText = stringJavaRDD.flatMap( l -> Arrays.asList( l.split( " " ) )
                .iterator() )
                .persist( new StorageLevel() );
        return ( linesText.mapToPair(
                ( PairFunction< String, String, Integer > ) x -> new Tuple2( x, 1 ) )
                .reduceByKey( ( Function2< Integer, Integer, Integer > ) ( x, y ) -> x + y ) );
    }



    /**
     * Breaks a text into its sentences and return its number.
     *
     * @param text, a String.
     * @return the number of words in the string separated by a space (//W).
     */
    public static int sentencesCount( String text )
    {
        ArrayList< String > count;
        if ( text != null && !( text.equals( "" ) ) ) {
            count = new ArrayList<>();
            BreakIterator bi = BreakIterator.getSentenceInstance( Locale.FRENCH );
            bi.setText( text );
            int start = 0;
            int end;
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
     * Applies a function (sentencesCount) which calculate the number of sentences in each row of the column "text"
     *
     * @param dataset, the dataframe containing all the data
     * @return the dataset plus the number of sentences
     */
    private Dataset< Row > numberOfSentences( Dataset< Row > dataset )
    {
        return dataset
                .withColumn( "nb_sentences", callUDF( "nbOfSentences", col( "text" ) ) );
    }


    /**
     * Function to add two columns : first the number of words per sample and the numbers of sentences per sample
     *
     * @param dataset the dataset
     * @return the upgraded dataset
     */
    public Dataset< Row > setMetric( Dataset< Row > dataset )
    {
        dataset.show();
        dataset = this.numberOfWords( dataset );// new columns with number of words and tokens
        dataset = this.numberOfSentences( dataset ); // new column with number of sentences
        dataset = dataset.withColumn( "words_per_sentences", col( "nb_words" ).divide( col( "nb_sentences" ) ) );
        dataset.show();
        Dataset< Row > z = dataset.persist().describe("nb_words", "nb_sentences", "words_per_sentences");
        z.show();
        Dataset< Row > racineDF = dataset.filter( col( "author" ).equalTo( "racinej" ) );
        racineDF.describe( "nb_words", "nb_sentences","words_per_sentences" ).show();
        Dataset< Row > moliereDF = dataset.filter( col( "author" ).equalTo( "moliere" ) );
        moliereDF.describe( "nb_words", "nb_sentences", "words_per_sentences" ).show();

        Dataset< Row > sum = dataset.persist().groupBy( "author" ).agg( sum( "nb_words" ), sum( "nb_sentences" ) );
        //sum.show();
        Long l = ( Long ) sum.select( "sum(nb_words)" ).collectAsList().get( 0 ).get( 0 );
        numberOfWordsClasse0 = l.intValue(); //racine
        l = ( Long ) sum.select( "sum(nb_words)" ).collectAsList().get( 1 ).get( 0 );
        numberOfWordsClasse1 = l.intValue(); //moliere
        IntDict d = wordsFrequency( wordsFrequency3( dataset ) );
        //d.print();
        //visualisation( d, ( numberOfWordsClasse0 + numberOfWordsClasse1 ) );
        IntDict m = wordsFrequency( wordsFrequency3( moliereDF ) );

        //visualisation( m, numberOfWordsClasse1 );
        IntDict r = wordsFrequency( wordsFrequency3( racineDF ) );
        //visualisation( r, numberOfWordsClasse0 );
        stopWords = setStopWords( m, r );
        return dataset;
    }

    /**
     *
     * @param moliere - words frequency for moliere class
     * @param racine - words frequency for racine class
     * @return a array of stop words
     */
   private String[] setStopWords( IntDict moliere, IntDict racine )
    {
        //remove most  frequent words
        ArrayList< String > sw = new ArrayList<>();
        for ( int i = 0; i <= 49; i++ ) {
            String s = moliere.keyArray()[ i ];
            if(racine.hasKey( s )) {
                double m = ( double ) moliere.get( s ) * 1000.0 / numberOfWordsClasse1;
                double r = ( double ) racine.get( s ) * 1000.0 / numberOfWordsClasse0;
                if ( ( 0.66 < ( m / r ) || ( m / r ) < 1.5 ) ){
                    sw.add( s );
                }
            }
        }
        String[] s = new String[ sw.size() ];
        sw.toArray( s );
        return s;
    }

    private void visualisation( IntDict intDict, int total )
    {

        setIntDict( intDict );
        setTotal( total );
        Visualization mySketch = new Visualization( this );
        PApplet.runSketch( processingArgs, mySketch );
    }


    private IntDict wordsFrequency( JavaPairRDD< String, Integer > wordsFreq )
    {
        int[] values = new int[ ( int ) wordsFreq.count() ];
        String[] keys = new String[ ( int ) wordsFreq.count() ];
        int i = 0;
        for ( Tuple2< String, Integer > couple : wordsFreq.collect() ) {
            values[ i ] = couple._2;
            keys[ i ] = couple._1;
            i++;
        }
        IntDict intDict = new IntDict( keys, values );
        intDict.sortValuesReverse();
        return intDict;

    }

    public String[] getStopWords()
    {
        return stopWords;
    }


}


