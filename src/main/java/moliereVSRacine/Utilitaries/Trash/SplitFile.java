package moliereVSRacine.Utilitaries.Trash;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.io.FileReader;
import java.util.ArrayList;

public class SplitFile
{

    private ArrayList< String > splittedFile;
    private String authorName;



    public SplitFile(File file, JavaSparkContext sc)
    {
        JavaRDD< String > lines = sc.textFile(file.getAbsolutePath());
        JavaPairRDD<String, String> file1 = sc.wholeTextFiles(file.getAbsolutePath());
        System.out.println(" file1 : " + file1.count());

        JavaRDD< String > copyright1 = lines.filter(theLines -> {
            return(theLines.contains("une"));
        });

        /**
         * Removes empty lines
         */
        lines = lines.filter(line -> !line.isEmpty());

        /**
         * Sets the name of the author of the split file
         */
        this.authorName = lines
                .filter(line -> line.contains("<AUTEUR"))
                .take(1)
                .get(0)
                .substring(8)
                .replace(">", "")
                .trim();

        System.out.println(authorName);
        int nbLine = (int) lines.count();
        System.out.println("nb line : " + nbLine);



        for (int i = 0; i<= nbLine ; i ++)
        {
            int nombreAleatoire = 1 + (int)(Math.random() * ((6 - 1) + 1));
            //Dataset<Row>
        }
        String line;
        try
        {
            BufferedReader br = new BufferedReader(new FileReader(file));
            //System.out.println("debut du br");
//            do {
//
//                line = br.readLine();
//                if (line != null) {
//                    //System.out.println(line);
//                    line = line.replace(";", " ");
//                    if (line.startsWith("ATOM")) {
//                        if (line.substring(12, 16).trim().equals("CA")
//                                || line.substring(12, 16).trim().equals("N")
//                                || line.substring(12, 16).trim().equals("O")
//                                || line.substring(12, 16).trim().equals("C")) {
//                            ATOM.add(line.substring(21,22)+//chainID
//                                    " "+line.substring(17, 20) +//residue name
//                                    " "+line.substring(12,16)+//Atom name
//                                    " " + line.substring(30, 54));//coordinates
//                        }
//
//                    } else if (line.startsWith("SEQRES")) {
//                        SEQRES.add(line.substring(6));
//                    } else if (line.startsWith("COMPND")) {
//                        COMPND.add(line.substring(10).trim());
//                    } else if (line.startsWith("TITLE")) {
//                        TITLE.add(line.substring(6));
//                    } else if (line.startsWith("HEADER")) {
//                        HEADER = line.substring(6);
//                        //System.out.println(HEADER);
//                    }
//                }
//            }
            //while (line != null ) ;
        }catch(FileNotFoundException fnfe){
            System.out.print("le fichier est introuvable");
        }catch(IOException ioe){
            System.out.print(ioe.getMessage());
        }


    }

    public static void main (String [] args ){
//        moliereVSRacine.Utilitaries.Trash.FileReader fileReader = new moliereVSRacine.Utilitaries.Trash.FileReader("http://abu.cnam.fr/cgi-bin/donner_unformated?avare2");
//        File file = fileReader.getFile();
        File file = new File("C:\\Users\\ctreb\\IdeaProjects\\MoliereVersusRacine\\src\\main\\resources\\avare.txt");
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("My app");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SplitFile splitFile = new SplitFile(file, sc);
    }
}
