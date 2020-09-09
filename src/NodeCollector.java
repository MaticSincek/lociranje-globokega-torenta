import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.KBucketEntry;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.messages.FindNodeResponse;
import lbms.plugins.mldht.kad.messages.SampleResponse;
import lbms.plugins.mldht.kad.messages.MessageBase;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

enum TT {
    BLACK,
    WHITE
}

/* ---PREVERJANJE BARVE INFOHASHOV NA OMREŽJU---
select (
    (select count (*) from (select DISTINCT ip,client_ID,infohash,infohash_color from dht_infohashes) a where infohash_color = 'W')
    /
    ((select count (*) from (select DISTINCT ip,client_ID,infohash,infohash_color from dht_infohashes) a) * 1.0)
) as black_percentage
*/

public class NodeCollector implements Runnable{

    DHT dht;
    ArrayList<KBucketEntry> knownList;
    ArrayList<KBucketEntry> notProcessed;
    ArrayList<String> DHTinfohashes;

    public NodeCollector(DHT dht, ArrayList<KBucketEntry> knownList, ArrayList<KBucketEntry> notProcessed, ArrayList<String> DHTinfohashes) {
        this.dht = dht;
        this.knownList = knownList;
        this.notProcessed = notProcessed;
        this.DHTinfohashes = DHTinfohashes;
    }

    @Override
    public void run() {

        dht.addIncomingMessageListener(new DHT.IncomingMessageListener() {
            @Override
            public void received(DHT dht, MessageBase messageBase) {
                if(messageBase instanceof FindNodeResponse) {

                    KBucketEntry kBucketEntry = new KBucketEntry(messageBase.getOrigin(), new Key(messageBase.getID().toString(false)));

                    boolean alreadyKnown = false;
                    if(knownList.contains(kBucketEntry))
                        alreadyKnown = true;

                    if(!alreadyKnown)
                        synchronized (knownList) {
                            knownList.add(kBucketEntry);
                        }

                    if(!alreadyKnown)
                        synchronized (notProcessed) {
                            notProcessed.add(kBucketEntry);
                        }
                } else if (messageBase instanceof SampleResponse) {

                    String sql;

                    for (Key k : ((SampleResponse) messageBase).getSamples()){

                        TT result = checkIfKeyIsPublic(k.toString(false));
                        sql = "";

                        if(result == TT.WHITE) {
                            sql +=  " ('"
                                    + k.toString(false) + "', '"
                                    + "W');";
                        } else if (result == TT.BLACK){
                            sql +=  " ('"
                                    + k.toString(false) + "', '"
                                    + "B');";
                        } else {
                            System.out.println("Error");
                        }

                        synchronized (DHTinfohashes) {
                            DHTinfohashes.add(sql);
                        }
                    }
                }
            }
        });
    }

    public OkHttpClient client = new OkHttpClient();

    public TT checkIfKeyIsPublic(String infohash) {

        String website = null;

        //ext.to
        Request request = new Request.Builder()
                .url("https://ext.to/search/?q=" + infohash)
                .build();

        try (Response response = client.newCall(request).execute()) {
            website =  response.body().string();
        } catch (IOException e){
            e.printStackTrace();
        }

        if(!website.contains("No results found"))
            return (TT.WHITE);

        //1337x.to
        request = new Request.Builder()
                .url("https://1337x.to/search/" + infohash + "/1/")
                .build();

        try (Response response = client.newCall(request).execute()) {
            website =  response.body().string();
        } catch (IOException e){
            e.printStackTrace();
        }

        if(!website.contains("No results were returned."))
            return (TT.WHITE);

        //btcache.me/
        request = new Request.Builder()
                .url("https://btcache.me/torrent/" + infohash)
                .build();

        try (Response response = client.newCall(request).execute()) {
            website =  response.body().string();
        } catch (IOException e){
            System.out.println("Napaka pri preverjanju infohasha: " + e.getMessage());
        }

        if(!website.contains("Error! Invalid INFO_HASH."))
            return (TT.WHITE);

        return TT.BLACK;
    }
}
