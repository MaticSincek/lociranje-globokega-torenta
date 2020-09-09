import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.XMLEvent;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;

public class Scrapper {

    static String databaseUrl = "jdbc:sqlite:D:/Nodes/infohashes.db";
    static Connection conn = null;

    static final ArrayList<String> torrentDiscoverySites = new ArrayList<>();

    public static void main(String[] args) {

        //websites
        torrentDiscoverySites.add("https://eztv.io/ezrss.xml");// 3
        torrentDiscoverySites.add("https://nyaa.si/?page=rss");//
        torrentDiscoverySites.add("https://www.limetorrents.info/rss/");// 10
        torrentDiscoverySites.add("https://torrentgalaxy.to/rss");// 5
        torrentDiscoverySites.add("https://rarbgway.org/rssdd.php?categories=14;15;16;17;21;22;42;44;45;46;47;48");// 1
        torrentDiscoverySites.add("https://rarbgway.org/rssdd.php?categories=18;19;41");
        torrentDiscoverySites.add("https://rarbgway.org/rssdd.php?categories=23;24;25;26");
        torrentDiscoverySites.add("https://rarbgway.org/rssdd.php?categories=27;28;29;30;31;32;40");
        torrentDiscoverySites.add("https://showrss.info/other/shows.rss");

        //DB
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (Exception e) {
            e.printStackTrace();
        }

        int iterations = 0;
        int newInfoHashes = 0;

        while (true) {

            //Resource
            URL rss = null;
            HttpURLConnection httpconn = null;

            try {
                System.out.println("Scrapping " + torrentDiscoverySites.get(iterations % 9));
                rss = new URL(torrentDiscoverySites.get(iterations%9));

                httpconn = (HttpURLConnection)rss.openConnection();

                httpconn.setRequestMethod  ("GET");
                httpconn.setRequestProperty("Content-Type",     "application/x-www-form-urlencoded");
                httpconn.setRequestProperty("Content-Language", "en-US");
                httpconn.setRequestProperty("User-Agent",       "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11");

                httpconn.setUseCaches(false);
                httpconn.setDoInput(true);
                httpconn.setDoOutput(true);

            } catch (Exception e) {
                e.printStackTrace();
            }

            BufferedReader reader = null;
            XMLInputFactory factory = null;
            XMLEventReader eventReader = null;

            try {
                reader = new BufferedReader(new InputStreamReader(httpconn.getInputStream()));
                factory = XMLInputFactory.newInstance();
                eventReader = factory.createXMLEventReader(reader);
            } catch (ConnectException ce){
                System.out.println("Problem connectiong, skipping...");
                break;
            } catch (Exception e) {
                System.out.println("Problem connectiong, skipping...");
                e.printStackTrace();
            }

            String title = "";
            String infohash = "";
            String pubdate = "";
            String category = "";
            String size = "";
            String leechers = "";
            String seeders = "";
            String scrapdate = "";

            LocalDateTime date = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss");
            scrapdate = date.format(formatter);

            try {
                while (eventReader.hasNext()) {

                    XMLEvent event = eventReader.nextEvent();

                    if (event.isStartElement()) {
                        String tagname = event.asStartElement().getName().getLocalPart();

                        switch(tagname) {
                            case "title":
                                title = returnXMLValue(eventReader);
                                break;
                            case "infoHash":
                                infohash = returnXMLValue(eventReader);
                                break;
                            case "pubDate":
                                pubdate = returnXMLValue(eventReader);
                                break;
                            case "category":
                                category = returnXMLValue(eventReader);
                                break;
                            case "leechers":
                                leechers = returnXMLValue(eventReader);
                                break;
                            case "seeders":
                                seeders = returnXMLValue(eventReader);
                                break;
                            case "size":
                                size = returnXMLValue(eventReader);
                                break;
                            case "guid":
                                if(iterations%9 <= 8 && iterations%9 >= 4 || iterations%9 == 3) {
                                    infohash = returnXMLValue(eventReader);
                                }

                                break;
                            case "enclosure":
                                if(iterations%9==2) {
                                    Iterator<Attribute> attribue = event.asStartElement().getAttributes();
                                    while(attribue.hasNext()){
                                        Attribute myAttribute = attribue.next();
                                        if(myAttribute.getName().toString().equals("url")){
                                            infohash = myAttribute.getValue().split("/torrent/")[1].split(".torrent?")[0];
                                        }
                                    }
                                }
                                break;
                        }
                    } else if (event.isEndElement()){

                        String tagname = event.asEndElement().getName().getLocalPart();

                        if(tagname == "item") {

                            connectToDB();

                            String sql = "SELECT * FROM infohashes WHERE infohash = ?";

                            try {
                                PreparedStatement ps1 = conn.prepareStatement(sql);
                                ps1.setString(1, infohash);

                                ResultSet rs = ps1.executeQuery();
                                if(rs.next()) {
                                    rs.close();
                                    ps1.close();
                                    disconnectFromDB();
                                    continue;
                                }
                                rs.close();
                                ps1.close();

                            } catch (Exception e) {
                                e.printStackTrace();
                            }

                            sql = "INSERT INTO infohashes(website, title, infohash, pubdate, category, leechers, seeders, size, scrapdate) VALUES (?,?,?,?,?,?,?,?,?)";

                            try {
                                PreparedStatement ps2 = conn.prepareStatement(sql);
                                ps2.setString(1, torrentDiscoverySites.get(iterations%9));
                                ps2.setString(2, title);
                                ps2.setString(3, infohash);
                                ps2.setString(4, pubdate);
                                ps2.setString(5, category);
                                ps2.setString(6, leechers);
                                ps2.setString(7, seeders);
                                ps2.setString(8, size);
                                ps2.setString(9, scrapdate);
                                ps2.executeUpdate();

                                newInfoHashes ++;
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }

                            disconnectFromDB();
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            iterations ++;

            if(iterations % 9 == 0){
                try {
                    System.out.println("New infohashes: " + newInfoHashes);
                    newInfoHashes = 0;
                    System.out.println("---------------------------------");
                    System.out.println("Waiting 1 minute before retrying.");
                    System.out.println("---------------------------------");
                    Thread.sleep(60000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static String returnXMLValue(XMLEventReader reader) {
        try {
            XMLEvent event = reader.nextEvent();
            return event.asCharacters().getData().toString();
        } catch (Exception e) {}
        return "";
    }

    public static void connectToDB() {
        try {
            conn = DriverManager.getConnection(databaseUrl);
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
