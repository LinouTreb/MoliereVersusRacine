package moliereVSRacine.Modelisation;

import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;

class FeatureExtraction implements Serializable
{

    private  Dataset [] featureDatasets ;

    /**
     *
     * @param dataset - the dataset
     * @param stopWords - the stop words list
     */
    Dataset<Row>  dataPrep(Dataset<Row> dataset, String[] stopWords){
        StringIndexer stringIndexer = new StringIndexer().
                setInputCol( "author" ).
                setOutputCol( "label" );
        StringIndexerModel stringIndexerModel = stringIndexer.fit(dataset);
        Dataset< Row > indexed =  stringIndexerModel.transform( dataset );

        StopWordsRemover remover = new StopWordsRemover()
                .setInputCol("words")
                .setOutputCol("filtered")
                .setStopWords( stopWords );
       return remover.transform(indexed);

}

    /**
     *
     * @param settings - the numbers of features for TFIDF and the vetor size for Words2Vec
     */
    void set(int[] settings, Dataset<Row> dataset){
        featureDatasets = new Dataset [2];
        featureDatasets[0] = setTfIdfFeatures( dataset, settings[0] );
        featureDatasets[1] = setWord2Vec(dataset,  settings[1] );
    }



    private Dataset<Row> setTfIdfFeatures(Dataset<Row> dataset,  int numFeatures ){
        HashingTF hashingTF = new HashingTF()
                .setNumFeatures(numFeatures)
                .setInputCol("filtered")
                .setOutputCol("rawFeatures");
        Dataset< Row > rawFeatures = hashingTF.transform( dataset).persist();

        IDF idf = new IDF().
                setInputCol(hashingTF.getOutputCol()).
                setOutputCol("TFfeatures");
        IDFModel idfModel = idf.fit(rawFeatures.persist());
        Dataset<Row> tfIdfData = idfModel.transform(rawFeatures);

        /* Assembles several features in one vector*/
        String [] colNames = {idf.getOutputCol(), "words_per_sentences"};
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(colNames)
                .setOutputCol("features");
        return assembler.transform( tfIdfData );
    }
    private Dataset<Row> setWord2Vec(Dataset<Row> dataset, int vectorSize)
    {
        dataset.show();
        // Learn a mapping from words to Vectors.
        Word2Vec word2Vec = new Word2Vec()
                .setInputCol("filtered")
                .setOutputCol("words2vec")
                .setVectorSize(vectorSize)
                .setMinCount(0);
        Word2VecModel model = word2Vec.fit(dataset.persist());
        Dataset<Row> result = model.transform(dataset);

        /* Assembles several features in one vector*/
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String []{word2Vec.getOutputCol(), "words_per_sentences"})
                .setOutputCol("features");
        return assembler.transform( result );

    }

    Dataset [] getFeatureDatasets()
    {
        return featureDatasets;
    }
}
