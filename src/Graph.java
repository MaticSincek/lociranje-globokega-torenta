import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;

public class Graph {

    static Connection conn = null;

    public static void connectToDB() {
        try {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bittorrent2","root","password");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void disconnectFromDB() {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static ArrayList<DHTClient> clients = new ArrayList<>();

    public static void main(String[] args) {

        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (Exception e) {
            System.out.println("Can't load com.mysql.jdbc.Driver class.");
        }

        connectToDB();

        String sql = "SELECT DISTINCT infohash, client_id FROM dht_infohashes";

        try {
            PreparedStatement ps1 = conn.prepareStatement(sql);

            ResultSet rs = ps1.executeQuery();
            while(rs.next()) {

                String infohash = rs.getString(1);
                String client_ID = rs.getString(2);

                boolean found = false;
                for (DHTClient cl : clients) {
                    if (cl.getID().equals(client_ID)) {
                        cl.getInfohashes().add(infohash);
                        found = true;
                        break;
                    }
                }

                if(!found) {
                    DHTClient n = new DHTClient(client_ID);
                    n.getInfohashes().add(infohash);
                    clients.add(n);
                }
            }
            rs.close();
            ps1.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        disconnectFromDB();

        for (DHTClient cl : clients) {
            for(String in : cl.getInfohashes()) {
                for (DHTClient d : getNodesWithInfohash(in)) {
                    if(!d.getID().equals(cl.getID())) {
                        cl.getNeighbours().add(d);
                    }
                }
            }
            //primer z malo infohashi za testiranje
            /*if (cl.getID().equals("FB15231C384F5E23C332AA6A40409572D12D6BB4")) {
                System.out.println("haha");
            }*/
        }
    }

    public static HashSet<DHTClient> getNodesWithInfohash(String infohash) {

        HashSet<DHTClient> result = new HashSet<DHTClient>();

        for (DHTClient cl : clients) {
            if (cl.getInfohashes().contains(infohash)) {
                result.add(cl);
            }
        }
        return result;
    }
}
