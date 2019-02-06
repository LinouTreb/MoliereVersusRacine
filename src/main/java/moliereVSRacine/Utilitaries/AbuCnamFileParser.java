package moliereVSRacine.Utilitaries;

import java.io.*;
import java.io.FileReader;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Locale;

public class AbuCnamFileParser {

    private File processFile;
    private ArrayList<String > textLines = new ArrayList<>();
    private String author;


    public AbuCnamFileParser(String path) {
        File file = new File(path);
        String text = "";
        String line;
        boolean startText = true;
        try {
            BufferedReader br = new BufferedReader(new FileReader(file.getAbsolutePath()));
            do {
                line = br.readLine();
                if (line != null) {
                    if (startText) {
                        if (line.startsWith("<AUTEUR")) {
                            author = line.substring(8).replace(">", "").trim();//the author's name
                        } else if (line.startsWith("------------------------- DEBUT DU FICHIER")) {
                            startText = false;
                            System.out.println("startext is false");
                        }
                    } else {
                        if (!line.startsWith("----")) {
                            text = text +  line.trim() + " ";
                        }
                    }

                }
            }
            while (line != null);
        }


        catch( FileNotFoundException fnfe )
        {
            System.out.print("the file doesn't exit");
        }
        catch(IOException ioe)
        {
            System.out.print(ioe.getMessage());
        }
        BreakIterator bi = BreakIterator.getSentenceInstance(Locale.FRENCH);
        bi.setText(text);
        int start = 0;
        int end = 0;
        while ((end = bi.next()) != BreakIterator.DONE) {
            textLines.add(text.substring(start, end) +"#"+getAuthor());
            //System.out.println(text.substring(start, end));
            start = end;
        }


    }

    public ArrayList getText() {
        return textLines;
    }

    public String getAuthor() {
        return author;
    }

    public static void main (String [] args ){
        AbuCnamFileParser a = new AbuCnamFileParser(
                "C:\\Users\\ctreb\\IdeaProjects\\MoliereVersusRacine\\src\\main\\resources\\avare.txt");
        System.out.println( a.getAuthor());

    }



}
