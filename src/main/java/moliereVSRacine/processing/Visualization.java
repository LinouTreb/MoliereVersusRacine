package moliereVSRacine.processing;

import moliereVSRacine.Modelisation.DataMetrics;
import processing.core.PApplet;
import processing.core.PFont;
import processing.data.IntDict;

import java.io.Serializable;

/**
 * The purpose of this type is to
 */
public class Visualization extends PApplet implements Serializable
{


private  IntDict intDict;
private int total;

    public static void main(String[] args) {

        PApplet.main("moliereVSRacine.processing.Visualization");
//moliereVSRacine.processing.Visualization
    }



    public Visualization( DataMetrics dataMetrics ){
    this.intDict = dataMetrics.getIntDict();
    this.total = dataMetrics.getTotal();}

    @Override
    public void settings() {
       size(400, 800);

//        PFont font;
//// The font must be located in the sketch's
//// "data" directory to load successfully
//        font = createFont("LetterGothicStd.ttf", 6);
//        textFont(font);
    }

    @Override
public void setup() {
//    this.fill(120,50,240);
//
}

    @Override
    public void draw()
    {
        if (this.intDict != null){
            background( 255 );
            int h = 30;
            // In order to iterate over every word in the dictionary,
            // first ask for an array of all of the keys.
            String[] keys = intDict.keyArray();

            for ( int i = 0; i < /*this.height / h*/ 30; i++ ) {
                // Look at each key one at a time and retrieve its count.
                String word = keys[ i ];
                //int count = intDict.get( word );
                float count2 =(float) intDict.get(word)*1000f/(float) total;

                this.fill( 51 );
                // Displaying a rectangle along with the count as a simple graph.
                this.rect( 0, i * 30, count2*5f , h - 2);
                this.fill( 0 );
                this.text( word + ": " + count2, 10 + count2*5f , i * h + h / 2 );
                this.stroke( 0 );
            }
        }
        loop();
                    }
    public void draw(IntDict intDict)
    {


        int h = 20;
        // In order to iterate over every word in the dictionary,
        // first ask for an array of all of the keys.
        String[] keys = intDict.keyArray();

        for ( int i = 0; i < this.height / h; i++ ) {
            // Look at each key one at a time and retrieve its count.
            String word = keys[ i ];
            int count = intDict.get( word );

            this.fill( 51 );
            // Displaying a rectangle along with the count as a simple graph.
            this.rect( 0, i * 20, count / 4, h - 4 );
            this.fill( 0 );
            this.text( word + ": " + count, 10 + count / 4, i * h + h / 2 );
            this.stroke( 0 );

        }
    }



    public void setIntDict( IntDict intDict )
    {
        this.intDict = intDict;
    }
}