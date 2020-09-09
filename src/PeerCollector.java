import lbms.plugins.mldht.DHTConfiguration;
import lbms.plugins.mldht.kad.*;
import lbms.plugins.mldht.kad.tasks.PeerLookupTask;

import java.io.File;
import java.net.SocketException;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

//dht_infohashes_new -> infohash_peers

public class PeerCollector {

    static DHT dht;
    static Node node;

    static Connection conn = null;

    public static void main(String[] args) {

        dht = new DHT(DHT.DHTtype.IPV4_DHT);

        dht.setLogLevel(DHT.LogLevel.Fatal);

        dht.addStatusListener(new DHTStatusListener() {
            @Override
            public void statusChanged(DHTStatus dhtStatus, DHTStatus dhtStatus1) {
                System.out.println("[STATUS]: " + dhtStatus.toString() + " -> " + dhtStatus1.toString());
            }
        });

        try {
            dht.start(new DHTConfiguration() {
                @Override
                public boolean isPersistingID() {
                    return false;
                }

                @Override
                public Path getStoragePath() {
                    File file = new File("D:/Nodes/PeerCollector");
                    return file.toPath();
                }

                @Override
                public int getListeningPort() {
                    return 53605;
                }

                @Override
                public boolean noRouterBootstrap() {
                    return false;
                }

                @Override
                public boolean allowMultiHoming() {
                    return false;
                }
            });
        } catch (SocketException e) {
            System.out.println(e.getMessage());
        }

        try {
            //wait 1 second for bootstrap
            java.util.concurrent.TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException ie) {
        }

        System.out.println("[INFO] Peer collector id: " + dht.getNode().getRootID());
        System.out.println("[INFO] Number of nodes in routing table: " + dht.getNode().getNumEntriesInRoutingTable());

        node = dht.getNode();

        //---------------------------------------------------------------------
        //                        BOOTSTRAP
        //---------------------------------------------------------------------

        /*for (Node.RoutingTableEntry routingTableEntry : node.table().list()) {
            for (KBucketEntry kBucketEntry : routingTableEntry.getBucket().getEntries()) {

                Key key = new Key(kBucketEntry.getID().toString(false));

                if (key != null) {

                    RPCServer rpc = dht.getServerManager().getRandomServer();

                    NodeLookup nodeLookup = new NodeLookup(key, rpc, dht.getNode(), false);
                    nodeLookup.start();

                    while(!nodeLookup.isFinished()){
                        try {
                            Thread.sleep(100);
                        } catch (Exception e) {
                            System.out.println(e.getMessage());
                        }
                    }

                    System.out.println("[INFO] Number of nodes in routing table: " + dht.getNode().getNumEntriesInRoutingTable());
                }
                //zbriši break če hočeš nodelookup na vseh bootstrap nodih
                break;
            }
            //zbriši break če hočeš nodelookup na vseh bootstrap nodih
            break;
        }*/

        HashMap<String, String> notProcessed = new HashMap<>();

        connectToDB();

        String sql = "SELECT infohash, color FROM dht_infohashes_new";

        try {
            PreparedStatement ps1 = conn.prepareStatement(sql);

            ResultSet rs = ps1.executeQuery();
            while(rs.next()) {
                notProcessed.put(rs.getString(1), rs.getString(2));
            }
            rs.close();
            ps1.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        disconnectFromDB();

        RPCServer rpc = dht.getServerManager().getRandomServer();

        int counter = 0;

        for(Map.Entry<String,String> entry : notProcessed.entrySet()) {

            counter++;

            System.out.println(counter + "/" + notProcessed.size());

            String ih = entry.getKey();
            String clr = entry.getValue();

            PeerLookupTask peerLookup = new PeerLookupTask(rpc, dht.getNode(), new Key(ih));
            peerLookup.start();

            while(!peerLookup.isFinished()){
                try {
                    Thread.sleep(100);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            ArrayList<String> items = new ArrayList<>();

            sql = "INSERT INTO infohash_peers (id, infohash, ip, infohash_color) VALUES";

            for (PeerAddressDBItem pa : peerLookup.getReturnedItems()) {
                sql +=  " (NULL, '"
                        + ih + "', '"
                        + pa.getAddressAsString() + "', '"
                        + clr + "'), ";
            }

            //peerlookup lahko vrne nič peerov, zato za nekatere infohashe ne bo vnosov v infohash_peers
            if (!sql.equals("INSERT INTO infohash_peers (id, infohash, ip, infohash_color) VALUES")) {
                sql = sql.substring(0, sql.length() - 2);

                connectToDB();

                try {
                    PreparedStatement ps = conn.prepareStatement(sql);
                    ps.executeUpdate();
                    ps.close();
                } catch (SQLException e) {
                    System.out.println("SQL: " + sql);
                    e.printStackTrace();
                }

                disconnectFromDB();
            }
        }
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
