package com.example.elasticsearch;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author wgm
 * @since 2021/4/27
 */
@Component
@Slf4j
public class BaseElasticService {

    final RestHighLevelClient restHighLevelClient;

    public BaseElasticService(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * 创建索引
     *
     * @param idxName
     * @param shards
     * @param replicas
     * @param idxDesc
     */
    @SneakyThrows
    public void createIndex(String idxName, int shards, int replicas, String idxDesc) {
        if (this.indexExist(idxName)) {
            log.error(" idxName={} already exits,idxSql={}", idxName, idxDesc);
            return;
        }
        CreateIndexRequest request = new CreateIndexRequest(idxName);
        request.settings(Settings.builder()
                .put("index.number_of_shards", shards)
                .put("index.number_of_replicas", replicas));
        CreateIndexResponse response = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        if (!response.isAcknowledged()) {
            // 没有响应
            throw new RuntimeException("createIndex acknowledged fail");
        }
    }

    /**
     * 查询索引
     *
     * @param idxName
     * @return
     */
    @SneakyThrows
    public GetIndexResponse getIndex(String idxName) {
        GetIndexRequest getIndexRequest = new GetIndexRequest(idxName);
        return restHighLevelClient.indices().get(getIndexRequest, RequestOptions.DEFAULT);
    }

    /**
     * 索引是否存在
     *
     * @param idxName
     * @return
     */
    @SneakyThrows
    private boolean indexExist(String idxName) {
        GetIndexRequest getIndexRequest = new GetIndexRequest(idxName);
        return restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
    }

    /**
     * 删除索引
     *
     * @param idxName
     */
    @SneakyThrows
    public void deleteIndex(String idxName) {
        if (!this.indexExist(idxName)) {
            log.error(" idxName={} not exits", idxName);
            return;
        }
        AcknowledgedResponse response = restHighLevelClient.indices().delete(new DeleteIndexRequest(idxName), RequestOptions.DEFAULT);
        if (!response.isAcknowledged()) {
            throw new RuntimeException("deleteIndex acknowledged fail");
        }
    }

    /**
     * 添加文档
     *
     * @param idxName
     * @param document
     * @return
     */
    @SneakyThrows
    public IndexResponse add(String idxName, Document document) {
        IndexRequest request = new IndexRequest(idxName);
        request.id(document.getId());
        request.source(document.getDate(), XContentType.JSON);
        return restHighLevelClient.index(request, RequestOptions.DEFAULT);
    }

    /**
     * 批量添加文档
     *
     * @param idxName
     * @param list
     * @return
     */
    @SneakyThrows
    public BulkResponse batchAdd(String idxName, List<Document> list) {
        BulkRequest request = new BulkRequest();
        list.forEach(item ->
                request.add(new IndexRequest(idxName).id(item.getId()).source(item.getDate(), XContentType.JSON)));
        return restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
    }

    /**
     * 搜索
     *
     * @param idxName
     * @param builder
     * @return
     */
    @SneakyThrows
    public List<String> search (String idxName, SearchSourceBuilder builder) {
        SearchRequest request = new SearchRequest(idxName);
        request.source(builder);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        SearchHit[] hits = response.getHits().getHits();
        return Arrays.stream(hits).map(SearchHit::getSourceAsString).collect(Collectors.toList());
    }

    /**
     * 批量删除
     *
     * @param idxName
     * @param idList
     * @param <T>
     * @return
     */
    @SneakyThrows
    public <T> BulkResponse deleteBatch(String idxName, Collection<T> idList) {
        BulkRequest request = new BulkRequest();
        idList.forEach(item -> request.add(new DeleteRequest(idxName, item.toString())));
        return restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
    }
}
