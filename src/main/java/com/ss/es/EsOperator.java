package com.ss.es;

import com.ss.parser.KeywordExtractor;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.get.GetResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by dolphineor on 2015-3-20.
 *
 * @deprecated
 */
@SuppressWarnings("unchecked")
public class EsOperator implements ElasticRequest {

    private final Logger LOGGER = LoggerFactory.getLogger(EsOperator.class);
    private final BlockingQueue<Map<String, Object>> insertRequestQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<Map<String, Object>> updateRequestQueue = new LinkedBlockingQueue<>();
    private final TransportClient client;


    public EsOperator(TransportClient client) {
        this.client = client;
        handleInsertRequest();
        handleUpdateRequest();
    }


    public void pushIndexRequest(Map<String, Object> request) {
        insertRequestQueue.offer(request);
    }

    public void pushUpdateRequest(Map<String, Object> request) {
        updateRequestQueue.offer(request);
    }

    private void handleInsertRequest() {
        Thread t = new Thread(() -> {
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            while (true) {
                Map<String, Object> requestMap = null;
                try {
                    requestMap = insertRequestQueue.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                if (requestMap == null)
                    continue;

                String[] locArr = (String[]) requestMap.get(CURR_ADDRESS);
                try {
                    if (locArr[0].contains(SEM_KEYWORD_IDENTIFIER)) {
                        // keyword parse
                        Map<String, Object> keywordInfoMap = KeywordExtractor.parse(locArr[0]);
                        if (!keywordInfoMap.isEmpty())
                            requestMap.putAll(keywordInfoMap);

                        URL url = new URL(locArr[0]);
                        locArr[0] = url.getProtocol() + "://" + url.getHost().split("/")[0];
                        requestMap.put(CURR_ADDRESS, new String[]{url.getProtocol() + "://" + url.getHost()});
                        requestMap.put(ENTRANCE, url.getProtocol() + "://" + url.getHost());
                        requestMap.put(DESTINATION_URL, locArr[0]);
                    } else
                        requestMap.put(ENTRANCE, locArr[0]);
                } catch (MalformedURLException e) {
                    e.printStackTrace();
                }

                IndexRequestBuilder builder = client.prepareIndex();

                try {
                    XContentBuilder contentBuilder = jsonBuilder().startObject();

                    // 设置field
                    for (Map.Entry<String, Object> entry : requestMap.entrySet()) {
                        if (ET.equals(entry.getKey())) {
                            continue;
                        }

                        contentBuilder.field(entry.getKey(), entry.getValue());
                    }

                    if (requestMap.containsKey(ET)) {

                        List<Map<String, String>> mapList = (ArrayList) requestMap.get(ET);

                        contentBuilder.startArray(ET);

                        for (Map<String, String> map : mapList) {
                            contentBuilder.startObject();
                            for (Map.Entry<String, String> entry : map.entrySet()) {
                                contentBuilder.field(entry.getKey(), entry.getValue());
                            }
                            contentBuilder.endObject();
                        }

                        contentBuilder.endArray();
                    }
                    contentBuilder.endObject();


                    builder.setIndex(VISITOR_PREFIX + LocalDate.now().toString());
                    builder.setType(requestMap.get(T).toString());
                    builder.setSource(contentBuilder);


                    bulkRequestBuilder.add(builder.request());
                } catch (IOException e) {
                    e.printStackTrace();
                }

                if (insertRequestQueue.isEmpty() && bulkRequestBuilder.numberOfActions() > 0) {
                    submitRequest(bulkRequestBuilder);
                    bulkRequestBuilder = client.prepareBulk();
                    continue;
                }

                if (bulkRequestBuilder.numberOfActions() == EsPools.getBulkRequestNumber()) {
                    submitRequest(bulkRequestBuilder);
                    bulkRequestBuilder = client.prepareBulk();
                }

            }
        });

        t.setName("handleVisitorInsert");
        t.start();
    }

    private void handleUpdateRequest() {
        Thread t = new Thread(() -> {
//            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            while (true) {
                Map<String, Object> requestMap = null;
                try {
                    requestMap = updateRequestQueue.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                if (requestMap == null)
                    continue;

                try {
                    XContentBuilder contentBuilder = jsonBuilder()
                            .startObject()
                            .field(CURR_ADDRESS, requestMap.get(CURR_ADDRESS))
                            .field(UNIX_TIME, requestMap.get(UNIX_TIME));

                    if (requestMap.containsKey(ET)) {
                        List<Map<String, String>> mapList = (ArrayList) requestMap.get(ET);

                        contentBuilder.startArray(ET);

                        for (Map<String, String> map : mapList) {
                            contentBuilder.startObject();
                            for (Map.Entry<String, String> entry : map.entrySet()) {
                                contentBuilder.field(entry.getKey(), entry.getValue());
                            }
                            contentBuilder.endObject();
                        }

                        contentBuilder.endArray();
                    }
                    contentBuilder.endObject();

                    UpdateResponse response = client.prepareUpdate()
                            .setIndex(requestMap.get(INDEX).toString())
                            .setType(requestMap.get(TYPE).toString())
                            .setId(requestMap.get(ID).toString())
                            .setDoc(contentBuilder)
                            .get();

                    // debug code
                    GetResult result = response.getGetResult();
                    Map<String, Object> resultSource = result.getSource();
                    List<String> resultLocations = (ArrayList<String>) resultSource.get(CURR_ADDRESS);
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("\n_id: " + result.getId() +
                                ", _version: " + result.getVersion() +
                                ", _source.tt: " + resultSource.get(TT) +
                                ", pv: " + resultLocations.size() +
                                ", the latest location: " + resultLocations.get(resultLocations.size() - 1)
                                + "\n");
                    }

//                    bulkRequestBuilder.add(client.prepareUpdate()
//                            .setIndex(requestMap.get(INDEX).toString())
//                            .setType(requestMap.get(TYPE).toString())
//                            .setId(requestMap.get(ID).toString())
//                            .setDoc(contentBuilder));
                } catch (IOException e) {
                    e.printStackTrace();
                }

//                if (updateRequestQueue.isEmpty() && bulkRequestBuilder.numberOfActions() > 0) {
//                    bulkRequestBuilder.get();
//                    bulkRequestBuilder = client.prepareBulk();
//                    continue;
//                }
//
//                if (bulkRequestBuilder.numberOfActions() == EsPools.getBulkRequestNumber()) {
//                    bulkRequestBuilder.get();
//                    bulkRequestBuilder = client.prepareBulk();
//                }

            }
        });

        t.setName("handleVisitorUpdate");
        t.start();
    }

    private void submitRequest(BulkRequestBuilder bulkRequestBuilder) {
        BulkResponse responses = bulkRequestBuilder.get();
        if (responses.hasFailures()) {
            System.out.println("Failure: " + responses.buildFailureMessage());
        }
    }

}
