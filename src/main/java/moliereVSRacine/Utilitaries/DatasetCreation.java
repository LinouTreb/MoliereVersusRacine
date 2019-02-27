package moliereVSRacine.Utilitaries;

import org.apache.spark.SparkConf;
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

import java.util.ArrayList;
import java.util.List;

public class DatasetCreation {


    public DatasetCreation(AbuCnamFileParser abuCnamFileParser, JavaSparkContext sc, SparkSession spark){
        // Create an RDD
        JavaRDD< String > javaRDD = sc.parallelize(abuCnamFileParser.getText());
        System.out.println("nb line  = " + javaRDD.count());

        //String author = abuCnamFileParser.getAuthor();


        // The schema is encoded in a string
        String chainSchema = "Text Author";

        // Generate the schema based on the string of schema
        List< StructField > fields = new ArrayList<>();
        for (String fieldName : chainSchema.split( " ") )
        {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add( field );
        }
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD (text) to Rows
        JavaRDD<Row> rowRDD = javaRDD.map((Function<String, Row>) record -> {
                String[] attributes = record.split("#");
                return RowFactory.create(attributes[0].trim(), attributes[1].trim() );

        });

// Apply the schema to the RDD
        Dataset<Row> dataFrame = spark.createDataFrame(rowRDD, schema);
        dataFrame.show(30);

    }

    public static void main (String [] args ){
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("My app");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("DatasetCreation")
                .config(sc.getConf())
                .getOrCreate();


        DatasetCreation dc = new DatasetCreation(
                new AbuCnamFileParser("C:\\Users\\ctreb\\IdeaProjects\\MoliereVersusRacine\\src\\main\\resources\\avare.txt"), sc,
                sparkSession);
    }
    //regexTokenized.withColumn( "SplitWords", concat_ws( " ",col("SplitWords" ) ));
}
