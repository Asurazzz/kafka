package com.ymj.es.controller;

import com.ymj.es.dao.*;
import com.ymj.es.entity.Message;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.Iterator;

/**
 * Created by admin on 17/6/1.
 */
@RestController
public class TestController {
    @Autowired
    private ArticleSearchRepository articleSearchRepository;

    @RequestMapping("/add")
    public void testSaveArticleIndex() {
        Message message = new Message();
        message.setSendTime(new Date());
        message.setMsg("elasticsearch based on lucene,"
                + "spring-data-elastichsearch based on elaticsearch"
                + ",this tutorial tell you how to integrete springboot with spring-data-elasticsearch");
        message.setId(2L);

        articleSearchRepository.save(message);
    }

    @RequestMapping("/query")
    public void testSearch() {
        String queryString = "springboot";//搜索关键字
        QueryStringQueryBuilder builder = new QueryStringQueryBuilder(queryString);
        Iterable<Message> searchResult = articleSearchRepository.search(builder);
        Iterator<Message> iterator = searchResult.iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
    }
}