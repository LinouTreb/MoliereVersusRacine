package moliereVSRacine.Utilitaries.Trash;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.net.URLConnection;

/**
 * Reads a file from an url and copies it in the tempdir.
 */
public class FileReader {

    /**
     * A tempory file copied from an url
     */
    private File file = null;

    /**
     * The number of the file, included in the file name.
     */
    private static int number  = 1;

    /**
     *
     * @param anURL the file url
     */
    public FileReader(String anURL ){
        try
        {
            URL url = new URL(anURL);
            URLConnection con = url.openConnection();
            BufferedInputStream in =     new BufferedInputStream(con.getInputStream());
            file = File.createTempFile(file+(Integer.toString(number)),".txt");
            FileOutputStream out =     new FileOutputStream(file);
            int i = 0;
            byte[] bytesIn = new byte[1024];

            while ((i = in.read(bytesIn)) >= 0)
            {
                out.write(bytesIn, 0, i);
            }
            out.close();
            in.close();
            number ++;
        }catch(Exception e)
        {
            e.printStackTrace();
        }
    }


    public File getFile() {
        return file;
    }
    public static void main ( String [] args )
    {
        new FileReader("http://abu.cnam.fr/cgi-bin/donner_unformated?avare2" );

    }
}
