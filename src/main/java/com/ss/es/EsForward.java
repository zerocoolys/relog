package com.ss.es;

import com.ss.main.SearchEngineParser;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.collect.Sets;

import java.io.UnsupportedEncodingException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yousheng on 15/3/16.
 */
public class EsForward implements ElasticRequest {

    // t -> TrackId; vid -> 访客唯一标识符； tt -> UV
    private static final String ACCESS_PREFIX = "access-";
    private static final String VISITOR_PREFIX = "visitor-";

    private BlockingQueue<Map<String, Object>> queue = new LinkedBlockingQueue<>();

    public EsForward(TransportClient client) {
        ConcurrentLinkedQueue<IndexRequest> requestQueue = new ConcurrentLinkedQueue<>();
        EsOperator esOperator = new EsOperator(client);
        preHandle(client, requestQueue, esOperator);
        handleRequest(client, requestQueue);
    }

    public void add(Map<String, Object> obj) {
        queue.add(obj);
    }

    private void preHandle(TransportClient client, ConcurrentLinkedQueue<IndexRequest> requestQueue, EsOperator esOperator) {

        Executors.newSingleThreadExecutor().execute(() -> {

            while (true) {
                IndexRequestBuilder builder = client.prepareIndex();

                Map<String, Object> mapSource = null;
                try {
                    mapSource = queue.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (mapSource == null)
                    continue;

                String trackId = mapSource.get(T).toString();
                String tt = mapSource.get(TT).toString();
                try {
                    String refer = mapSource.get(RF).toString();
                    if ("-".equals(refer)) {
                        mapSource.put(SE, "-");
                        mapSource.put(KW, "-");
                        mapSource.put(RF_TYPE, 1);
                    } else {
                        String[] sk = SearchEngineParser.getSK(java.net.URLDecoder.decode(refer, "UTF-8"));
                        mapSource.put(SE, sk[0]);
                        mapSource.put(KW, sk[1]);
                        if ("-".equals(sk[0]) && "-".equals(sk[1]))
                            mapSource.put(RF_TYPE, 3);
                        else
                            mapSource.put(RF_TYPE, 2);
                    }
                } catch (NullPointerException | UnsupportedEncodingException e) {
                    e.printStackTrace();
                }

                LocalDate localDate = LocalDate.now();
                builder.setType(trackId);
                builder.setSource(mapSource);

                Map<String, Object> doc = visitorExists(client.prepareSearch(), VISITOR_PREFIX + localDate.toString(), trackId, tt);
                builder.setIndex(ACCESS_PREFIX + localDate.toString());
                requestQueue.add(builder.request());

                if (doc.isEmpty()) {
                    builder = client.prepareIndex(VISITOR_PREFIX + localDate.toString(), trackId);

                    Set<String> currAddress = Sets.newHashSet(mapSource.remove(CURR_ADDRESS).toString());
                    Set<Long> utime = Sets.newHashSet(Long.valueOf(mapSource.remove(UNIX_TIME).toString()));
                    mapSource.put(CURR_ADDRESS, currAddress.toArray(new String[currAddress.size()]));
                    mapSource.put(UNIX_TIME, utime.toArray(new Long[utime.size()]));

                    builder.setSource(mapSource);
                    esOperator.pushIndexRequest(builder.request());
                } else {
                    builder = client.prepareIndex(VISITOR_PREFIX + localDate.toString(), trackId);

                    List<String> currAddress = (ArrayList) doc.get(CURR_ADDRESS);
                    List<Long> utime = (ArrayList) doc.get(UNIX_TIME);

                    currAddress.add(mapSource.remove(CURR_ADDRESS).toString());
                    utime.add(Long.valueOf(mapSource.remove(UNIX_TIME).toString()));
                    mapSource.put(CURR_ADDRESS, currAddress.toArray(new String[currAddress.size()]));
                    mapSource.put(UNIX_TIME, utime.toArray(new Long[utime.size()]));
                    esOperator.pushUpdateRequest(doc);
                }
            }

        });

    }

    private void handleRequest(TransportClient client, ConcurrentLinkedQueue<IndexRequest> requestQueue) {
        Executors.newSingleThreadExecutor().execute(() -> {
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            while (true) {
                if (requestQueue.isEmpty() && bulkRequestBuilder.numberOfActions() > 0) {
                    bulkRequestBuilder.get();
                    bulkRequestBuilder = client.prepareBulk();
                } else if (!requestQueue.isEmpty()) {
                    bulkRequestBuilder.add(requestQueue.poll());
                    if (bulkRequestBuilder.numberOfActions() == EsPools.getBulkRequestNumber()) {
                        bulkRequestBuilder.get();
                        bulkRequestBuilder = client.prepareBulk();
                    }
                }

            }
        });
    }

}
