package moliereVSRacine;

import moliereVSRacine.Modelisation.DataMetrics;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.mutable.WrappedArray;

import javax.swing.*;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Collections;
import java.util.List;

import static moliereVSRacine.Modelisation.DataMetrics.sentencesCount;
import static moliereVSRacine.Modelisation.DataMetrics.wordsCount;


public class MainClass extends JFrame implements ActionListener {

    private JButton submit;
    private JButton clear;
    private JTextArea text;
    private JLabel label;
    private SparkSession spark;

     MainClass( final SparkSession spark) {
        this.spark = spark;
        setSize(600, 300);
        setBackground(Color.cyan);
        setTitle("Molière ou racine");

        setLocationRelativeTo(null);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        setLayout(new BorderLayout(0, 0));

        JPanel jPanel = new JPanel();
        jPanel.setLayout(new BorderLayout());
        label = new JLabel();
        label.setSize(400, 50);
        text = new JTextArea();
        text.setBackground(Color.lightGray);
        this.add(text, BorderLayout.CENTER);
        this.add(jPanel, BorderLayout.SOUTH);
        submit = new JButton("Submit");
        clear = new JButton("Clear");
        submit.addActionListener(this);
        clear.addActionListener(this);
        jPanel.add(label, BorderLayout.WEST);
        jPanel.add(clear, BorderLayout.NORTH);
        jPanel.add(submit, BorderLayout.EAST);

        this.setVisible(true);


    }

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("My app");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");
        SparkSession spark = SparkSession
                .builder()
                .appName("DatasetCreation")
                .config(sc.getConf())
                .getOrCreate();
        // Registration of User Defined Function
        spark.udf().register("wordsCount", (WrappedArray<String> s) -> wordsCount(s), DataTypes.IntegerType);
        spark.udf().register("nbOfSentences", (String s) -> sentencesCount(s), DataTypes.IntegerType);
        new MainClass(spark);
    }

    @Override
    public void actionPerformed(ActionEvent e) {
        if (e.getSource() == this.submit) {
            if (this.text.getText() != null && !this.text.getText().equals("")) {
                this.label.setText(" ");
                String s = this.text.getText();
                List<Row> data = Collections.singletonList(RowFactory.create(s));
                StructType schema = new StructType(new StructField[]{
                        new StructField("text", DataTypes.StringType, false, Metadata.empty())});
                Dataset<Row> sentenceData = spark.createDataFrame(data, schema);
                sentenceData.show();
                DataMetrics dataMetrics = new DataMetrics();
                sentenceData = dataMetrics.setMetric1(sentenceData);
                CrossValidatorModel pipelineModel = CrossValidatorModel
                        .load("C:\\Users\\Dani-Linou\\IdeaProjects\\MoliereVersusRacine\\src\\main\\resources\\model");
                Dataset<Row> r = pipelineModel.transform(sentenceData);
                r.select("prediction", "probability").show();
                System.out.println(r.select("prediction").toJavaRDD().collect().get(0).get(0).getClass());
                System.out.println(r.select("prediction").toJavaRDD().collect().get(0).get(0));
                if ((double) r.select("prediction").toJavaRDD().collect().get(0).get(0) == 0.0) {
                    this.label.setText("Ceci est un texte de Racine ");
                } else {
                    this.label.setText("Ceci est un texte de Molière ");
                }
            } else {
                this.label.setText("C'est vide !");
            }
        } else if (e.getSource() == this.clear) {
            this.text.setText("");
            this.label.setText(" ");
        }
    }
}

