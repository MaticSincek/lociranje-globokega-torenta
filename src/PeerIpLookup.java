import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import okhttp3.*;
import org.json.simple.JSONArray;

import java.io.IOException;
import java.sql.*;
import java.sql.Connection;

public class PeerIpLookup {

    static final String API_ADDRESS = "http://ip-api.com/batch?fields=query,country,isp,as";

    static Connection conn = null;

    public static void main(String[] args) {

        OkHttpClient client = new OkHttpClient();

        connectToDB();

        String sql = "SELECT ip FROM client_colors where isp is null";

        try {
            PreparedStatement ps1 = conn.prepareStatement(sql);
            ResultSet rs = ps1.executeQuery();

            int c = 0;

            while(rs.next()) {

                JSONArray jsonArray = new JSONArray();

                for (int i = 0; i < 100; i++) {
                    if(rs.next()) {
                        jsonArray.add(rs.getString(1));
                    }
                }

                MediaType JSON = MediaType.parse("application/json; charset=utf-8");

                RequestBody body = RequestBody.create(jsonArray.toString(), JSON);

                Request request = new Request.Builder()
                        .url(API_ADDRESS)
                        .post(body)
                        .build();

                String responseJson = "";

                try (Response response = client.newCall(request).execute()) {
                    responseJson =  response.body().string();
                    //responseJson = StringEscapeUtils.unescapeJava(responseJson);
                } catch (IOException e){
                    e.printStackTrace();
                }

                JsonArray responseArray = null;

                try {
                    JsonParser parser = new JsonParser();
                    JsonElement tmp = parser.parse(responseJson);
                    responseArray = tmp.getAsJsonArray();
                } catch (Exception e) {
                    System.out.println("Napaka pri dekodiranju: " + e.getMessage());
                }

                sql = "UPDATE client_colors SET country = ?, isp = ?, asn = ? WHERE ip = ?";

                for (JsonElement el : responseArray) {

                    try {

                        String country = el.getAsJsonObject().get("country").getAsString();
                        String isp =     el.getAsJsonObject().get("isp").getAsString();
                        String asn =     el.getAsJsonObject().get("as").getAsString();
                        String ip =      el.getAsJsonObject().get("query").getAsString();

                        PreparedStatement ps2 = conn.prepareStatement(sql);
                        ps2.setString(1, country);
                        ps2.setString(2, isp);
                        ps2.setString(3, asn);
                        ps2.setString(4, ip);
                        ps2.executeUpdate();
                        ps2.close();

                    } catch (NullPointerException npe) {
                        System.out.println("Napaka - problem z identificiranjem IP naslova.");
                    } catch (SQLException se) {
                        System.out.println("Napaka pri izvajanju sql - " + se.getMessage());
                    } catch (Exception e) {
                        System.out.println("Napaka - " + e.getMessage());
                    }
                }

                c += jsonArray.size();
                System.out.println(c + "/8300");
            }

            rs.close();

        } catch (Exception e) {
            e.printStackTrace();
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
