package moliereVSRacine.Utilitaries;

import java.util.ArrayList;

public class Play {

    private String author;
    private ArrayList< String > acts;

    public Play()
    {
        this.acts = new ArrayList<>();
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getAuthor() {
        return author;
    }

    public ArrayList<String> getActs() {
        return acts;
    }

    /**
     * Add the label (author) to each acts.
     */
    public void labellisation(){
        for (int i = 0; i<= getActs().size()-1; i++){

            getActs().set(i, getActs().get(i)+ "#"+ getAuthor());System.out.println(getActs().get(i));
        }
    }
}
