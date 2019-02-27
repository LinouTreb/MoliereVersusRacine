package moliereVSRacine;

import processing.core.PApplet;

public class Pdisplay


{

    private PApplet p ;
    public Pdisplay(){
        this.p = new PApplet();
    }

    public void displayHistogramm(int [] data){
        p.settings();
        p.background( 255 );
        p.stroke(0);
        for (int i = 0; i<=data.length -1; i++) {
            // Use array of ints to set the color and height of each rectangle.
            p.rect(i*20, 0, 20, data[i]);
        }
        p.noLoop();


    }

    public void settings() {
        p.size(400, 400);
    }

}
