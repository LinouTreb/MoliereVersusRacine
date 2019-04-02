package moliereVSRacine.dataset;

import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.io.FileReader;
import java.util.ArrayList;


/**
 * Split a AbuCnam play files into its acts. Removes all accents.
 */
public class FileToPlay
{


    private String author;
    private File file;
    private ArrayList< String > data;


    /**
     *
     * @param path - the path of the file.
     */
    public FileToPlay( String path )
    {
        this.file = new File( path );
        fileToPlay();
    }

    /**
     * Reads the file and splits the file and transform it into a  {@link Play}
     */
    private void fileToPlay()
    {
        Play play = new Play();
        String line;
        int i = -1;
        boolean startText = true;
        try {
            BufferedReader br = new BufferedReader( new FileReader( file.getAbsolutePath() ) );
            do {
                line = br.readLine();
                if ( line != null ) {
                    line = StringUtils.stripAccents( line );  // removes all accents
                    if ( startText ) {
                        if ( line.startsWith( "<IDENT_AUTEURS" ) ) {
                            //the author's name
                            author = line.substring( 15 ).replace( ">", "" ).trim();
                        } else if ( line.startsWith( "------------------------- DEBUT DU FICHIER" ) ) {
                            startText = false;
                        }
                    } else if ( !( line.contains( "---" ) || line.isEmpty() ) ) {
                        if ( line.toLowerCase().startsWith( "acte " ) ) {
                            i++;
                            play.getActs().add( "" );
                        } else {
                            if ( i >= 0 ) {
                                if ( !( line.toLowerCase().startsWith( "scene" ) ) ) {
                                    play.getActs().set( i, play.getActs().get( i ) + " " + line );
                                }
                            }
                        }
                    }
                }
            }
            while ( line != null );
        } catch ( FileNotFoundException fnfe ) {
            System.out.print( "the file doesn't exit" );
        } catch ( IOException ioe ) {
            System.out.print( ioe.getMessage() );
        }
        play.setAuthor( this.getAuthor() );
        play.labellisation();
        this.data = play.getActs();
    }

    private String getAuthor()
    {
        return author;
    }

    public ArrayList< String > getData()
    {
        return data;
    }
}
