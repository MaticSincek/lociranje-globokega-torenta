import lbms.plugins.mldht.kad.DHT;

import java.util.ArrayList;
import java.util.HashSet;

public class DHTClient {

    private String ID;
    private HashSet<String> infohashes;
    private ArrayList<DHTClient> neighbours;

    public DHTClient(String ID) {
        this.ID = ID;
        this.infohashes = new HashSet<String>();
        this.neighbours = new ArrayList<DHTClient>();
    }

    public String getID() {
        return ID;
    }

    public void setID(String ID) {
        this.ID = ID;
    }

    public HashSet<String> getInfohashes() {
        return infohashes;
    }

    public void setInfohashes(HashSet<String> infohashes) {
        this.infohashes = infohashes;
    }

    public ArrayList<DHTClient> getNeighbours() {
        return neighbours;
    }

    public void setNeighbours(ArrayList<DHTClient> neighbours) {
        this.neighbours = neighbours;
    }
}
