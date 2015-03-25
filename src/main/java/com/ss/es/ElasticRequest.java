package com.ss.es;

import com.ss.main.Constants;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by baizz on 2015-3-23.
 */
public interface ElasticRequest extends Constants {

    String ID = "_id";
    String INDEX = "index";
    String TYPE = "_type";


    TransportClient getEsClient();


    default GetRequestBuilder getGetRequestBuilder() {
        return getEsClient().prepareGet();
    }

    default SearchRequestBuilder getSearchRequestBuilder() {
        return getEsClient().prepareSearch();
    }

    default IndexRequestBuilder getIndexRequestBuilder(String index, String type) {
        IndexRequestBuilder indexRequestBuilder = getEsClient().prepareIndex();
        indexRequestBuilder.setIndex(index).setType(type);
        return indexRequestBuilder;
    }

    default UpdateRequestBuilder getUpdateRequestBuilder() {
        return getEsClient().prepareUpdate();
    }

    default CountRequestBuilder getCountRequestBuilder(String index) {
        return getEsClient().prepareCount(index);
    }

    default DeleteRequestBuilder getDeleteRequestBuilder(String index, String type) {
        DeleteRequestBuilder deleteRequestBuilder = getEsClient().prepareDelete();
        deleteRequestBuilder.setIndex(index).setType(type);
        return deleteRequestBuilder;
    }

    default DeleteByQueryRequestBuilder getDeleteByQueryRequestBuilder() {
        return getEsClient().prepareDeleteByQuery();
    }

    default BulkRequestBuilder getBulkRequestBuilder() {
        return getEsClient().prepareBulk();
    }

    default Map<String, Object> visitorExists(String index, String type, String tt) {
        IndicesExistsRequest request = new IndicesExistsRequest(index);
        boolean isExists = getEsClient().admin().indices().exists(request).actionGet().isExists();
        if (!isExists)
            return Collections.emptyMap();

        SearchRequestBuilder searchRequestBuilder = getSearchRequestBuilder().setIndices(index).setTypes(type);
        SearchResponse response = searchRequestBuilder.setQuery(QueryBuilders.termQuery(TT, tt)).get();
        SearchHits hits = response.getHits();

        if (hits.getTotalHits() == 1) {
            SearchHit hit = hits.getAt(0);
            Map<String, Object> doc = new HashMap<>();
            doc.put(ID, hit.getId());
            doc.put(INDEX, hit.getIndex());
            doc.put(TYPE, hit.getType());
            hit.getSource().forEach((k, v) -> {
                switch (k) {
                    case CURR_ADDRESS:
                        doc.put(k, v);
                        break;
                    case UNIX_TIME:
                        doc.put(k, v);
                        break;
                    default:
                        break;
                }
            });
            return doc;
        } else {
            return Collections.emptyMap();
        }
    }
}
