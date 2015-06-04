package com.ss.quartz;

import com.ss.es.EsPools;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.client.IndicesAdminClient;

import java.time.LocalDate;
import java.util.ResourceBundle;
import java.util.TimerTask;

/**
 * Created by dolphineor on 2015-4-28.
 */
public class IndexTask extends TimerTask {
    @Override
    public void run() {
        EsPools.getEsClient().forEach(client -> {
            IndicesAdminClient indicesClient = client.admin().indices();
            ResourceBundle bundle = ResourceBundle.getBundle("indexPrefix");
            bundle.keySet().forEach(k -> {
                String index = bundle.getString(k) + LocalDate.now().plusDays(1).toString();
                if (!indicesClient.exists(new IndicesExistsRequest(index)).actionGet().isExists())
                    indicesClient.create(new CreateIndexRequest(index)).actionGet();
            });
        });
    }
}
