package com.ss.es;

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

/**
 * Created by baizz on 2015-3-23.
 */
public interface ElasticRequest {

    public static final String _ID = "_id";

    public static final String TT = "tt";   // UV


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

    default boolean visitorExists(String index, String type, String tt) {
        IndicesExistsRequest request = new IndicesExistsRequest(index);
        boolean isExists = getEsClient().admin().indices().exists(request).actionGet().isExists();
        if (!isExists)
            return false;

        SearchRequestBuilder searchRequestBuilder = getSearchRequestBuilder().setIndices(index).setTypes(type);
        SearchResponse response = searchRequestBuilder.setQuery(QueryBuilders.termQuery(TT, tt)).get();
        long hits = response.getHits().getTotalHits();
        return hits == 1;
    }
}
