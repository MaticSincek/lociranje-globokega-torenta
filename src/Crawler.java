import lbms.plugins.mldht.DHTConfiguration;
import lbms.plugins.mldht.kad.*;
import lbms.plugins.mldht.kad.DHT.DHTtype;
import lbms.plugins.mldht.kad.Node.RoutingTableEntry;
import lbms.plugins.mldht.kad.messages.SampleRequest;
import lbms.plugins.mldht.kad.tasks.KeyspaceSampler;
import lbms.plugins.mldht.kad.tasks.NodeLookup;

import java.io.File;
import java.net.SocketException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

//--------------------SET UP------------------------------------
// Skopirati moramo mapo Nodes na direktorij, ki obstaja
// Spremeniti moramo hardcodano lokacijo v programu
//--------------------------------------------------------------

public class Crawler {

    static DHT dht;
    static Node node;

    static Connection conn = null;

    public static void main(String[] args) {

        dht = new DHT(DHTtype.IPV4_DHT);

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
                    return true;
                }

                @Override
                public Path getStoragePath() {
                    File file = new File("d:/Nodes/Crawler");
                    return file.toPath();
                }

                @Override
                public int getListeningPort() {
                    return 53604;
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

        System.out.println("[INFO] Crawler id: " + dht.getNode().getRootID());
        System.out.println("[INFO] Number of nodes in routing table: " + dht.getNode().getNumEntriesInRoutingTable());

        node = dht.getNode();

        //---------------------------------------------------------------------
        //                        BOOTSTRAP
        //---------------------------------------------------------------------

        for (RoutingTableEntry routingTableEntry : node.table().list()) {
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
        }

        /*int limit = 0;
        int i = 0;
        for (RoutingTableEntry routingTableEntry : node.table().list()) {
            for (KBucketEntry kBucketEntry : routingTableEntry.getBucket().getEntries()) {

                Key key = new Key("1EB64E7ADD04A2D8DCEE7B6A381908A6DA89945F");

                if (key != null) {
                    i++;

                    RPCServer rpc = dht.getServerManager().getRandomServer();

                    NodeLookup nodeLookup = new NodeLookup(key, rpc, dht.getNode(), false);
                    nodeLookup.start();

                    while(!nodeLookup.isFinished()){}

                    logger.info("[INFO] Number of nodes in routing table: " + dht.getNode().getNumEntriesInRoutingTable());
                }
                if(i >= limit) break;
            }
            if(i >= limit) break;
        }*/

        ArrayList<KBucketEntry> notProcessed = new ArrayList<>();
        ArrayList<KBucketEntry> knownList = new ArrayList<>();

        for (RoutingTableEntry routingTableEntry : node.table().list()) {
            for (KBucketEntry kBucketEntry : routingTableEntry.getBucket().getEntries()) {
                //if (kBucketEntry.getID().toString(false).toCharArray()[0] == dht.getOurID().toString(false).toCharArray()[0]) {
                    notProcessed.add(kBucketEntry);
                    knownList.add(kBucketEntry);
                //}
            }
        }

        ArrayList<String> infohashesDHT = new ArrayList<>();

        Thread thread1 = new Thread(new NodeCollector(dht, knownList, notProcessed, infohashesDHT));
        thread1.start();

        RPCServer rpc = dht.getServerManager().getRandomServer();

        while (notProcessed.size() != 0) {

            KBucketEntry toProcess;

            synchronized (notProcessed) {
                toProcess = notProcessed.get(0);
                notProcessed.remove(toProcess);
            }

            String keystr = new StringBuilder(toProcess.getID().toString(false)).reverse().toString();
            toProcess = new KBucketEntry(toProcess.getAddress(), new Key(keystr));

            Key key = new Key(keystr);
            Prefix prefix  = new Prefix(key, 150);
            key = prefix.first();

            NodeLookup nodeLookup = new NodeLookup(key, rpc, dht.getNode(), false);
            nodeLookup.start();

            while(!nodeLookup.isFinished()){
                try {
                    Thread.sleep(100);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }

            RPCCall rpcCall = new RPCCall(new SampleRequest(new Key(keystr)));
            rpcCall.getRequest().setDestination(toProcess.getAddress());

            rpc.doCall(rpcCall);

            KeyspaceSampler sampler = new KeyspaceSampler(rpc, dht.getNode(), prefix, nodeLookup, null);
            sampler.start();

            while(!sampler.isFinished()){
                try {
                    Thread.sleep(100);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }

            if(infohashesDHT.size() > 20) {

                String sql = "INSERT INTO dht_infohashes_new (infohash, color) VALUES";

                ArrayList<String> temp = new ArrayList<>();

                synchronized (infohashesDHT) {
                    for (String s : infohashesDHT) {
                        temp.add(s);
                    }

                    infohashesDHT.removeAll(infohashesDHT);
                }

                connectToDB();

                for (String t : temp) {

                    try {
                        PreparedStatement ps = conn.prepareStatement(sql + t);
                        ps.executeUpdate();
                        ps.close();
                    } catch (SQLException e) {
                        System.out.println("Napaka pri vstavljanju: " + e.getMessage());
                    }
                }

                disconnectFromDB();
            }

            System.out.println("[INFO] Number of pending infohashes for insertion: " + infohashesDHT.size());
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
