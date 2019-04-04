package moliereVSRacine.Modelisation;


import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;

class FeatureExtraction implements Serializable {

    public Dataset<Row> getFeatured() {
        return featured;
    }

    private Dataset<Row> featured;
    private Dataset<Row> dataset;
    private String[] stopWords;

    FeatureExtraction(Dataset<Row> dataset, String[] stopWords){
        this.dataset = dataset;
        this.stopWords = stopWords;
    }

    /**
     *
     */
    void dataPrep() {
        StringIndexer stringIndexer = new StringIndexer()
                .setInputCol("author")
                .setOutputCol("label");

        StopWordsRemover remover = new StopWordsRemover()
                .setInputCol("words")
                .setOutputCol("filtered")
                .setStopWords(this.stopWords);


        HashingTF hashingTF = new HashingTF()
                .setNumFeatures(2000)
                .setInputCol(remover.getOutputCol())
                .setOutputCol("rawFeatures");

        IDF idf = new IDF().
                setInputCol(hashingTF.getOutputCol()).
                setOutputCol("TFfeatures");


        String[] colNames = {idf.getOutputCol(), "words_per_sentences"};
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(colNames)
                .setOutputCol("features");
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{stringIndexer, remover, hashingTF, idf, assembler});
        PipelineModel pipelineModel = pipeline.fit(this.dataset);

        this.featured =  pipelineModel.transform(this.dataset);

    }




}
