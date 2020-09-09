import lbms.plugins.mldht.DHTConfiguration;
import lbms.plugins.mldht.kad.*;
import lbms.plugins.mldht.kad.DHT.DHTtype;
import lbms.plugins.mldht.kad.Node.RoutingTableEntry;
import lbms.plugins.mldht.kad.tasks.NodeLookup;
import lbms.plugins.mldht.kad.tasks.PeerLookupTask;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.SocketException;
import java.nio.file.Path;
import java.util.ArrayList;

public class PeerCollectorTest {

    static DHT dht;
    static Node node;

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(PeerCollectorTest.class);

        dht = new DHT(DHTtype.IPV4_DHT);

        dht.setLogLevel(DHT.LogLevel.Fatal);

        dht.addStatusListener(new DHTStatusListener() {
            @Override
            public void statusChanged(DHTStatus dhtStatus, DHTStatus dhtStatus1) {
                logger.info("[STATUS]: " + dhtStatus.toString() + " -> " + dhtStatus1.toString());
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
                    File file = new File("d:/Nodes/PeerCollectorTest");
                    return file.toPath();
                }

                @Override
                public int getListeningPort() {
                    return 53606;
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
            logger.info(e.getMessage());
        }

        try {
            //wait 1 second for bootstrap
            java.util.concurrent.TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException ie) {
        }

        logger.info("[INFO] Number of nodes in routing table: " + dht.getNode().getNumEntriesInRoutingTable());

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
                            logger.info(e.getMessage());
                        }
                    }

                    logger.info("[INFO] Number of nodes in routing table: " + dht.getNode().getNumEntriesInRoutingTable());
                }
            }
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

        RPCServer rpc = dht.getServerManager().getRandomServer();

        Graph<String, DefaultEdge> g = new SimpleGraph<>(DefaultEdge.class);

        ArrayList <String> keys = new ArrayList<>();
        keys.add("4BC33D94E823ED158DB3A58E518A0A26760CFD60");
        keys.add("419A49D6B5404FE4BA4DDB359EF304AB81B7B858");
        keys.add("1EB64E7ADD04A2D8DCEE7B6A381908A6DA89945F");
        keys.add("4BC33D94E823ED158DB3A58E518A0A26760CFD60");

        int duplicates = 0;

        for(String key : keys) {
            PeerLookupTask peerLookup = new PeerLookupTask(rpc, dht.getNode(), new Key(key));
            peerLookup.start();

            while(!peerLookup.isFinished()){
                try {
                    Thread.sleep(100);
                } catch (Exception e) {
                    logger.info(e.getMessage());
                }
            }

            ArrayList<String> items = new ArrayList<>();

            for (PeerAddressDBItem pa : peerLookup.getReturnedItems()) {
                items.add(pa.getAddressAsString());

                if(!g.containsVertex(pa.getAddressAsString())) {
                    g.addVertex(pa.getAddressAsString());
                } else {
                    duplicates ++;
                }
            }

            logger.info("[PeerLookup] Returned " + items.size() + " peers for infohash " + key);

            for(String v1 : items) {
                for(String v2 : items) {
                    if(!v1.equals(v2)) {
                        g.addEdge(v1, v2);
                    }
                }
            }

        }
        System.out.println(duplicates);
    }
}


