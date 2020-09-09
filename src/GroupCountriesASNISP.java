import java.sql.*;
import java.util.Hashtable;

public class GroupCountriesASNISP {

    static Connection conn = null;

    public static void main(String[] args) {
        processASN();
    }

    //isti asn ima za eno stevilko razlicna imena - tako sem izgubil pribljizno 10 vrstic v bazi
    public static void processASN(){

        Hashtable<String, Float> sums = new Hashtable();
        Hashtable<String, Integer> counts = new Hashtable();

        connectToDB();

        String sql = "SELECT asn, color, infohash_count from client_colors order by asn ";

        try {
            PreparedStatement ps1 = conn.prepareStatement(sql);
            ResultSet rs = ps1.executeQuery();

            String c = "zzz___zzz_aeiou";

            while (rs.next()) {

                String asn = rs.getString(1);
                float color = rs.getFloat(2);
                int infohash_count = rs.getInt(3);

                if(!c.equals(asn)) {
                    c = asn;
                    counts.put(c, infohash_count);
                    sums.put(c, color * infohash_count);
                } else {
                    counts.put(c, counts.get(c) + infohash_count);
                    sums.put(c, sums.get(c) + color * infohash_count);
                }
            }
            rs.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        disconnectFromDB();

        connectToDB();

        for (String c : sums.keySet()) {

            sql = "INSERT INTO asns (asn, name, color, ihcount) VALUES (?, ?, ?, ?)";

            String asnID = c.split(" ")[0];
            asnID = asnID.substring(2, asnID.length());

            try {
                PreparedStatement ps2 = conn.prepareStatement(sql);
                ps2.setInt(1, Integer.parseInt(asnID));
                ps2.setString(2, c);
                ps2.setFloat(3, sums.get(c)/counts.get(c));
                ps2.setInt(4, counts.get(c));
                ps2.executeUpdate();
                ps2.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        disconnectFromDB();
    }

    public static void processISP(){

        Hashtable<String, Float> sums = new Hashtable();
        Hashtable<String, Integer> counts = new Hashtable();

        connectToDB();

        String sql = "SELECT isp, color, infohash_count FROM client_colors order by isp";

        try {
            PreparedStatement ps1 = conn.prepareStatement(sql);
            ResultSet rs = ps1.executeQuery();

            String c = "";

            while (rs.next()) {

                String isp = rs.getString(1);
                float color = rs.getFloat(2);
                int infohash_count = rs.getInt(3);

                if(!c.equals(isp)) {
                    c = isp;
                    counts.put(c, infohash_count);
                    sums.put(c, color * infohash_count);
                } else {
                    counts.put(c, counts.get(c) + infohash_count);
                    sums.put(c, sums.get(c) + color * infohash_count);
                }
            }
            rs.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        disconnectFromDB();

        connectToDB();

        for (String c : sums.keySet()) {

            sql = "INSERT INTO isps (id, name, color, ihcount) VALUES (NULL, ?, ?, ?)";

            try {
                PreparedStatement ps2 = conn.prepareStatement(sql);
                ps2.setString(1, c);
                ps2.setFloat(2, sums.get(c)/counts.get(c));
                ps2.setInt(3, counts.get(c));
                ps2.executeUpdate();
                ps2.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        disconnectFromDB();
    }

    public static void processCountries(){

        Hashtable<String, Float> sums = new Hashtable();
        Hashtable<String, Integer> counts = new Hashtable();

        connectToDB();

        String sql = "SELECT country, color, infohash_count FROM client_colors order by country";

        try {
            PreparedStatement ps1 = conn.prepareStatement(sql);
            ResultSet rs = ps1.executeQuery();

            String c = "";

            while (rs.next()) {

                String country = rs.getString(1);
                float color = rs.getFloat(2);
                int infohash_count = rs.getInt(3);

                if(!c.equals(country)) {
                    c = country;
                    counts.put(c, infohash_count);
                    sums.put(c, color * infohash_count);
                } else {
                    counts.put(c, counts.get(c) + infohash_count);
                    sums.put(c, sums.get(c) + color * infohash_count);
                }
            }
            rs.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        disconnectFromDB();

        connectToDB();

        for (String c : sums.keySet()) {

            sql = "INSERT INTO countries (id, name, color, ihcount) VALUES (NULL, ?, ?, ?)";

            try {
                PreparedStatement ps2 = conn.prepareStatement(sql);
                ps2.setString(1, c);
                ps2.setFloat(2, sums.get(c)/counts.get(c));
                ps2.setInt(3, counts.get(c));
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
