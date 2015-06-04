package com.ss.es;

import com.ss.main.Constants;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by baizz on 2015-3-23.
 *
 * @deprecated
 */
public interface ElasticRequest extends Constants {

    default Map<String, Object> visitorExists(SearchRequestBuilder searchBuilder, String index, String type, String tt) {
        SearchRequestBuilder searchRequestBuilder = searchBuilder.setIndices(index).setTypes(type);
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
                    case ET:
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
