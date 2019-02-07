package moliereVSRacine;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.mutable.WrappedArray;

import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;


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
     *
     * @param dataset, the dataset to process.
     * @param colName, the name of the column containing the different classes/categories.
     * @param className, the name of the class of interest.
     * @return
     */
    public int samplesClassCount(Dataset<Row> dataset,String colName, String className){

        return (int) dataset.filter( col( colName).like(className) ).count();

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

    public double getMedianNumber(Dataset <Row > dataset, String colName){
        Column col =  dataset.col(colName);
        dataset.describe(colName).show();
        return 0.2;
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
        SparkSession spark = SparkSession
                .builder()
                .appName( "DatasetCreation" )
                .config( sc.getConf() )
                .getOrCreate();
        sc.setLogLevel("WARN");

        List< Row > data = Arrays.asList(
                RowFactory.create(
                        "Les moments me sont chers, écoutez-moi, Thésée.\n" +
                                "C'est moi qui sur ce fils chaste et respectueux\n" +
                                "Osai jeter un oeil profane, incestueux.\n" +
                                "Le ciel mit dans mon sein une flamme funeste ;\n" +
                                "La détestable OEnone a conduit tout le reste.\n" +
                                "Elle a craint qu'Hippolyte, instruit de ma fureur,\n" +
                                "Ne découvrît un feu qui lui faisait horreur.\n" +
                                "La perfide, abusant de ma faiblesse extrême,\n" +
                                "S'est hâtée à vos yeux de l'accuser lui-même.\n" +
                                "Elle s'en est punie, et fuyant mon courroux,\n" +
                                "A cherché dans les flots un supplice trop doux.\n" +
                                "Le fer aurait déjà tranché ma destinée ;\n" +
                                "Mais je laissais gémir la vertu soupçonnée.\n" +
                                "J'ai voulu, devant vous exposant mes remords,\n" +
                                "Par un chemin plus lent descendre chez les morts.\n" +
                                "J'ai pris, j'ai fait couler dans mes brûlantes veines\n" +
                                "Un poison que Médée apporta dans Athènes.\n" +
                                "Déjà jusqu'à mon coeur le venin parvenu\n" +
                                "Dans ce coeur expirant jette un froid inconnu ;\n" +
                                "Déjà je ne vois plus qu'à travers un nuage\n" +
                                "Et le ciel, et l'époux que ma présence outrage ;\n" +
                                "Et la mort, à mes yeux dérobant la clarté,\n" +
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
        Dataset< Row > s = dm.numberOfSentences( sentenceDataFrame, "Text" );
        s.show();
        Dataset< Row > v = dm.numberOfwords( s, "Text" );
        v.show();

        System.out.println( "nb of classes : " + dm.classCount( v, "Author" ) );
        System.out.println( "nb of data for classes moliere : "+ dm.samplesClassCount( v, "Author", "moliere" ));
        System.out.println( "nb of data for classes racine : "+ dm.samplesClassCount( v, "Author", "racine" ));
        System.out.println(" median : " + dm.getMedianNumber( v, "nb_Words" ));
        v.describe( "Author" ).show();
        DataFrameStatFunctions dataFrameStatFunctions = new DataFrameStatFunctions( v );
        //dataFrameStatFunctions.freqItems( col("Words", "Author" )
    }
}



//TODO Number of words per sample:
//TODO Distribution of sample length: Distribution showing the number of words per sample in the dataset.
//TODO Frequency distribution of words: Distribution showing the frequency (number of occurrences) of each word in the dataset.