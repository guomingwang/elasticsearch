package com.example.elasticsearch;

import lombok.extern.slf4j.Slf4j;
import net.minidev.json.JSONObject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author wgm
 * @since 2021/4/27
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class BaseElasticServiceTest {

    @Autowired
    public BaseElasticService baseElasticService;

    final String INDEX_NAME = "index";

    @Test
    public void indexApiTest() {
        baseElasticService.createIndex(INDEX_NAME, 3, 2);
        baseElasticService.getIndex(INDEX_NAME);
//        baseElasticService.deleteIndex(INDEX_NAME);
    }

    @Test
    public void addApiTest() {
        HashMap<String, Object> map = new HashMap<>();
        map.put("name", "alex");
        map.put("age", 30);
        map.put("location", "beijing");
        Document document = new Document("1", JSONObject.toJSONString(map));
        baseElasticService.add(INDEX_NAME, document);

        ArrayList<Document> docList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            HashMap<String, Object> mapq = new HashMap<>();
            mapq.put("name", "alex");
            mapq.put("age", 30);
            mapq.put("location", "beijing");
            mapq.put("number", i);
            Document doc = new Document(String.valueOf(i), JSONObject.toJSONString(mapq));
            docList.add(doc);
        }
        baseElasticService.batchAdd(INDEX_NAME, docList);
    }

    @Test
    public void searchApiTest() {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.termQuery("name", "alex"));
        builder.from(0);
        builder.size(2);
        builder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        List<String> list = baseElasticService.search(INDEX_NAME, builder);
        System.out.println(list);
    }

    @Test
    public void deleteApiTest() {
        List<String> idList = Arrays.asList("1");
        baseElasticService.deleteBatch(INDEX_NAME, idList);
    }
}
