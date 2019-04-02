package moliereVSRacine.dataset;

import java.util.ArrayList;

/**
 * A play contains an AbuCnam file split into its acts
 */
 class Play {

    private String author;
    private ArrayList< String > acts;

    Play()
    {
        this.acts = new ArrayList<>();
    }

    void setAuthor(String author) {
        this.author = author;
    }

    private String getAuthor() {
        return author;
    }

    ArrayList<String> getActs() {
        return acts;
    }

    /**
     * Add the label (#author) to each acts.
     */
    void labellisation(){
        for (int i = 0; i<= getActs().size()-1; i++){
            getActs().set(i, getActs().get(i)+ "#"+ getAuthor());
        }
    }
}
