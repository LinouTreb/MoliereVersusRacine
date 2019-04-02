package moliereVSRacine.dataset;

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
import scala.Serializable;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DatasetCreation implements Serializable
{

    private Dataset <Row> dataset;


    /**
     * Constructor
     * @param sc - the program's {@link JavaSparkContext}
     * @param spark - the program's {@link SparkSession}
     */
    public DatasetCreation( final JavaSparkContext sc, final SparkSession spark )
    {

            File folder = new File( Objects.requireNonNull( DatasetCreation.class.getClassLoader().getResource( "corpus" ) ).getPath() );
            ArrayList< String > data = new ArrayList<>( 10 );
            this.listFilesForFolder( folder, data );


            // Create an JavaRDD
            JavaRDD< String > stringJavaRDD = sc.parallelize( data );


            // The schema is encoded in a string
            String chainSchema = "text author";

            // Generate the schema based on the string of schema
            List< StructField > fields = new ArrayList<>();
            for ( String fieldName : chainSchema.split( " " ) ) {
                StructField field = DataTypes.createStructField( fieldName, DataTypes.StringType, true );
                fields.add( field );
            }
            StructType schema = DataTypes.createStructType( fields );

            // Convert records of the RDD (text) to Rows
            JavaRDD< Row > rowData = stringJavaRDD.map( ( Function< String, Row > ) record ->
            {
                String[] attributes = record.split( "#" );
                return RowFactory.create( attributes[ 0 ].trim(), attributes[ 1 ].trim() );

            } );
            dataset = spark.createDataFrame( rowData, schema ).persist();
    }

    /**
     *
     * @param folder - the folder containing all AbuCnam files
     * @param data - the files data as an {@link ArrayList}
     */
    private void listFilesForFolder( final File folder,ArrayList< String > data  )
    {
        for ( final File fileEntry : Objects.requireNonNull( folder.listFiles() ) )
        {
            FileToPlay f = new FileToPlay( fileEntry.getAbsolutePath() );
            data.addAll( f.getData() );
        }
    }

    public Dataset< Row > getDataset()
    {
        return dataset;
    }
}
