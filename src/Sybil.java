import lbms.plugins.mldht.DHTConfiguration;
import lbms.plugins.mldht.kad.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.SocketException;
import java.nio.file.Path;
import java.util.ArrayList;

public class Sybil implements Runnable{

    private String configurationNumber;
    private DHT dht;
    ArrayList<KBucketEntry> knownList;

    public Sybil(String configurationNumber, ArrayList<KBucketEntry> knownList) {
        this.configurationNumber = configurationNumber;
        this.knownList = knownList;
    }

    @Override
    public void run() {
        Logger logger = LoggerFactory.getLogger(Sybil.class);

        dht = new DHT(DHT.DHTtype.IPV4_DHT);

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
                    File file = new File("d:/Nodes/Sybil/" + configurationNumber);
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
            logger.info(e.getMessage());
        }

        try {
            //wait 1 second for bootstrap
            java.util.concurrent.TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException ie) {
        }

        logger.info("[INFO] Sybil#" + configurationNumber + " with id: " + dht.getNode().getRootID() + " online");

        RPCServer rpc = dht.getServerManager().getRandomServer();

        for (int i = 0;; i++) {
            while (knownList.size() <= (i + 1)) {
                try {
                    Thread.sleep(100);
                } catch (Exception e) {
                    logger.info(e.getMessage());
                }
            }
        }
    }

    public DHT getDht() {
        return dht;
    }

    public void setDht(DHT dht) {
        this.dht = dht;
    }

    public String getConfigurationNumber() {
        return configurationNumber;
    }
}
