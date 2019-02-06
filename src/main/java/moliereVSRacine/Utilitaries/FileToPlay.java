package moliereVSRacine.Utilitaries;

import java.io.*;
import java.io.FileReader;
import java.util.ArrayList;


/**
 * Split a AbuCnam play files into its acts.
 */
public class FileToPlay {


    private String author;
    private File file;
    private ArrayList< String > data;


    public FileToPlay(String path) {
        System.out.println(path);
        this.file = new File(path);
        fileToPlay();


    }

    public void fileToPlay(){
        Play play = new Play();
        String line;

        int i = -1;
        boolean startText = true;
        boolean act = false;
        try {
            BufferedReader br = new BufferedReader(new FileReader(file.getAbsolutePath()));
            do {
                line = br.readLine();
                if (line != null) {
                    if (startText) {
                        if (line.startsWith( "<IDENT_AUTEURS" ) )
                        {
                            author = line.substring(15).replace(">", "").trim();//the author's name
                        } else if (line.startsWith("------------------------- DEBUT DU FICHIER" ) )
                        {
                            startText = false;
                            System.out.println( "startext is false" );
                        }
                    } else {
                        if (line.startsWith("----") || line.isEmpty()){

                        }
                        //if ( (!line.contains( "----" ))|| ( !line.isEmpty()) || ( !line.equals( "" ) ) )
                        else {
                            if ( line.startsWith( "ACTE" ) )
                            {
                                i++;
                                act = true;
                                play.getActs().add( "" );
                            }
                            else
                            {
                                if( i >= 0 && act )
                                    play.getActs().set(i, play.getActs().get(i ) + " "+ line);
                            }
                        }
                    }

                }
            }
            while ( line != null );
        }


        catch( FileNotFoundException fnfe )
        {
            System.out.print( "the file doesn't exit" );
        }
        catch( IOException ioe )
        {
            System.out.print( ioe.getMessage() );
        }
        play.setAuthor(getAuthor());
        play.labellisation();
        this.data = play.getActs();
    }

    public String getAuthor() {
        return author;
    }

    public ArrayList<String> getData() {
        return data;
    }

    public static void main (String [] args ){
        FileToPlay a = new FileToPlay(
                "C:\\Users\\ctreb\\IdeaProjects\\MoliereVersusRacine\\src\\main\\resources\\avare.txt");
        System.out.println( a.getAuthor());

    }



}
