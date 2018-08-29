package com.flaming.test.es.service;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flaming.test.es.entity.Data;
import lombok.SneakyThrows;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Author Flaming
 * @Contact xiaolin.tang@palmaplus.com
 * @date 2018/7/21 15:29
 */
@Service
public class EsService {

    @Autowired
    private ObjectMapper objectMapper;

    private List<Data> dataList;

    public EsService(){
        this.dataList = new ArrayList<>();
        this.dataList.add(new Data("01", "中国上海", 15));
        this.dataList.add(new Data("02", "欧洲法国",3));
        this.dataList.add(new Data("03", "中国辽宁抚顺", 26));
        this.dataList.add(new Data("04", "中国辽宁大连", 41));
        this.dataList.add(new Data("05", "中国上海法国街", 30));
    }


    @SneakyThrows
    public void esServiceInsert(){
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));

        BulkRequest bulkRequest = new BulkRequest();

        for(Data data : this.dataList){
            String json = JSON.toJSONString(data);
            IndexRequest indexRequest = new IndexRequest("estest", "data");
            indexRequest.source(json, XContentType.JSON);
            bulkRequest.add(indexRequest);
        }

        BulkResponse bulkResponse = client.bulk(bulkRequest);
        System.out.println(1);
    }

    @SneakyThrows
    public void esServiceDeteleAll(){
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http"))
        );
        SearchRequest searchRequest = new SearchRequest("estest");
        searchRequest.types("data");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest);
        List<SearchHit> hits = Arrays.asList(searchResponse.getHits().getHits());
        List<String> ids = new ArrayList<>();
        hits.stream().map( hit -> ids.add(hit.getId()) ).collect(Collectors.toList());

        if(ids.isEmpty()){
            System.out.println("id list is empty!");
            return;
        }

        BulkRequest bulkRequest = new BulkRequest();
        for(String id : ids){
            DeleteRequest deleteRequest = new DeleteRequest("estest", "data", id);
            bulkRequest.add(deleteRequest);
        }

        BulkResponse bulkResponse = client.bulk(bulkRequest);
        System.out.println(1);
    }

    @SneakyThrows
    public void esServiceDelete(String name){
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));
        SearchRequest searchRequest = new SearchRequest("estest");
        searchRequest.types("data");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("name", name));
        searchSourceBuilder.size(100);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        List<String> ids = new ArrayList<>();
        for(SearchHit hit : hits){
            ids.add(hit.getId());
            System.out.println(" " + hit.getSourceAsString());
        }

        if(ids.isEmpty()){
            System.out.println("id list is empty!");
            return;
        }
        BulkRequest request = new BulkRequest();
        for(String id : ids){
            DeleteRequest deleteRequest = new DeleteRequest("estest", "data", id);
            request.add(deleteRequest);
        }
        BulkResponse responses = client.bulk(request);
        System.out.println(1);
    }


    public void esServiceSearch(String keyword){
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));

        SearchRequest searchRequest = new SearchRequest("estest");
        searchRequest.types("data");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(100);

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        WildcardQueryBuilder wildcardQueryBuilder = QueryBuilders.wildcardQuery("name",  "*" + keyword.toLowerCase() + "*");

        //boolQueryBuilder.should(QueryBuilders.matchQuery("name", keyword));
        //boolQueryBuilder.should(wildcardQueryBuilder);
        //boolQueryBuilder.should(QueryBuilders.multiMatchQuery(keyword).analyzer("ik_max_word").minimumShouldMatch("60%"));
        searchSourceBuilder.query(wildcardQueryBuilder);
        searchRequest.source(searchSourceBuilder);

        try {
            SearchResponse searchResponse = client.search(searchRequest);
            List<SearchHit> searchHits = Arrays.asList(searchResponse.getHits().getHits());

            List<Data> dataList = searchHits.stream().map(searchHit -> {
                try {
                    Data data = objectMapper.readValue(searchHit.getSourceAsString(), Data.class);
                    return data;
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }).collect(Collectors.toList());
            System.out.println(dataList.size());
            for (Data d : dataList) {
                System.out.println(d.getName());
            }

        } catch (Exception e){
            e.printStackTrace();
        } finally {
            try {
                client.close();
            } catch (Exception e){
                e.printStackTrace();
            }
        }

    }

    @SneakyThrows
    public void esServiceSearchAll(){
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("localhost", 9200, "http")
        ));

        SearchRequest request = new SearchRequest("estest");
        request.types("data");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        request.source(sourceBuilder);

        SearchResponse response = client.search(request);

        List<SearchHit> hits = Arrays.asList(response.getHits().getHits());

        for(SearchHit hit : hits){
            Data data = objectMapper.readValue(hit.getSourceAsString(), Data.class);
            System.out.println("name >> " + data.getName() + " number >> " + data.getNumber());
        }
    }

}
