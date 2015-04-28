package com.ss.quartz;

//import com.ss.es.EsPools;
//import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
//import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
//import org.elasticsearch.client.IndicesAdminClient;
//import org.quartz.Job;
//import org.quartz.JobExecutionContext;
//import org.quartz.JobExecutionException;
//
//import java.time.LocalDate;
//import java.util.ResourceBundle;

/**
 * Created by baizz on 2015-3-26.
 *
 * @deprecated
 */
public class IndexJob /*implements Job*/ {

//    @Override
//    public void execute(JobExecutionContext context) throws JobExecutionException {
//        EsPools.getEsClient().forEach(client -> {
//            IndicesAdminClient indicesClient = client.admin().indices();
//            ResourceBundle bundle = ResourceBundle.getBundle("indexPrefix");
//            bundle.keySet().forEach(k -> {
//                String index = bundle.getString(k) + LocalDate.now().plusDays(1).toString();
//                if (!indicesClient.exists(new IndicesExistsRequest(index)).actionGet().isExists())
//                    indicesClient.create(new CreateIndexRequest(index)).actionGet();
//            });
//        });
//    }
}
