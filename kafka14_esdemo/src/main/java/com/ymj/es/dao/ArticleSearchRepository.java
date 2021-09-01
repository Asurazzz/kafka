package com.ymj.es.dao;

import com.ymj.es.entity.Message;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

public interface ArticleSearchRepository extends ElasticsearchRepository<Message, Long> {
}