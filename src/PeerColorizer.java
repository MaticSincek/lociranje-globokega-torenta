import java.sql.*;
import java.util.Hashtable;

public class PeerColorizer {

    //infohash_peers -> client_colors (brez lookupov)

    static Connection conn = null;

    public static void main(String[] args) {

        Hashtable<String, Integer> all = new Hashtable();
        Hashtable<String, Integer> whites = new Hashtable();

        connectToDB();

        String sql = "SELECT ip, infohash_color FROM infohash_peers order by ip";

        try {
            PreparedStatement ps1 = conn.prepareStatement(sql);
            ResultSet rs = ps1.executeQuery();

            String tmp = "";

            while (rs.next()) {

                String ip = rs.getString(1);
                String clr = rs.getString(2);

                if(!tmp.equals(ip)) {
                    tmp = ip;
                    all.put(tmp, 1);
                    whites.put(tmp, (clr.equals("W")) ? 1 : 0);
                } else {
                    all.put(tmp, all.get(tmp) + 1);
                    whites.put(tmp, (clr.equals("W")) ? whites.get(tmp) + 1 : whites.get(tmp));
                }
            }
            rs.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        disconnectFromDB();

        connectToDB();

        for (String tmp : whites.keySet()) {

            sql = "INSERT INTO client_colors (id, ip, color, infohash_count) VALUES (null, ?, ?, ?)";

            try {
                PreparedStatement ps2 = conn.prepareStatement(sql);
                ps2.setString(1, tmp);
                ps2.setFloat(2, (float)whites.get(tmp)/all.get(tmp));
                ps2.setInt(3, all.get(tmp));
                ps2.executeUpdate();
                ps2.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        disconnectFromDB();
    }

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
}
