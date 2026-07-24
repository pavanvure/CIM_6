package com.oati.topology.top.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oati.topology.top.model.mongo.OutagecardStatusInfo;
import com.oati.topology.top.repository.OutagecardStatusInfoRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reads OutagecardStatusInfo documents from MongoDB by taskId, builds a
 * header/payload envelope, and publishes either to Kafka or to a REST API.
 *
 * Supports:
 *  - Optional pagination when reading from Mongo (paginationRequired flag)
 *  - Choice of publish target (Kafka or REST API) via PublishTarget enum
 *  - Automatic message splitting so no single Kafka message exceeds
 *    MAX_PAYLOAD_BYTES (kept safely under Kafka's default 1MB message.max.bytes)
 */
@Service
public class OutageBatchService {

    private static final Logger log = LoggerFactory.getLogger(OutageBatchService.class);

    private final OutagecardStatusInfoRepository repository;
    private final RestTemplate restTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    private static final String TARGET_API_URL = "http://localhost:8080/api/generic/process";
    private static final String KAFKA_TOPIC = "outage-status-topic";
    private static final int BATCH_SIZE = 50;

    // Keep well under Kafka's default 1MB (message.max.bytes / max.request.size)
    private static final long MAX_PAYLOAD_BYTES = 900 * 1024; // 900 KB safety margin

    public enum PublishTarget {
        KAFKA, REST_API
    }

    public OutageBatchService(OutagecardStatusInfoRepository repository,
                               RestTemplate restTemplate,
                               KafkaTemplate<String, String> kafkaTemplate,
                               ObjectMapper objectMapper) {
        this.repository = repository;
        this.restTemplate = restTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    // ------------------------------------------------------------------
    // Entry point
    // ------------------------------------------------------------------

    public void processOutageData(String taskId, boolean paginationRequired, PublishTarget target) {
        AtomicInteger batchSeq = new AtomicInteger(1);
        if (paginationRequired) {
            processInBatches(taskId, target, batchSeq);
        } else {
            processAllAtOnce(taskId, target, batchSeq);
        }
    }

    // ------------------------------------------------------------------
    // Mongo reads (paged vs. all-at-once)
    // ------------------------------------------------------------------

    private void processInBatches(String taskId, PublishTarget target, AtomicInteger batchSeq) {
        long totalCount = repository.countByTaskId(taskId);
        if (totalCount == 0) {
            log.warn("No records found for taskId: {}", taskId);
            return;
        }

        int pageNumber = 0;
        Slice<OutagecardStatusInfo> slice;

        do {
            Pageable pageable = PageRequest.of(pageNumber, BATCH_SIZE, Sort.by("_id"));
            slice = repository.findByTaskId(taskId, pageable);

            if (!slice.getContent().isEmpty()) {
                boolean isLastMongoPage = !slice.hasNext();
                splitAndPublish(slice.getContent(), totalCount, target, taskId, batchSeq, isLastMongoPage);
            }
            pageNumber++;
        } while (slice.hasNext());
    }

    private void processAllAtOnce(String taskId, PublishTarget target, AtomicInteger batchSeq) {
        List<OutagecardStatusInfo> all = repository.findByTaskId(taskId);
        if (all.isEmpty()) {
            log.warn("No records found for taskId: {}", taskId);
            return;
        }
        splitAndPublish(all, all.size(), target, taskId, batchSeq, true);
    }

    // ------------------------------------------------------------------
    // Size-based splitting
    // ------------------------------------------------------------------

    /**
     * Recursively halves the record list until the serialized message
     * fits within MAX_PAYLOAD_BYTES, then publishes each resulting chunk.
     * isLastChunkOfMongoPage tells us whether THIS mongo page is the final one overall,
     * so the very last chunk produced from it can be flagged isLastBatch = true.
     */
    private void splitAndPublish(List<OutagecardStatusInfo> records, long totalCount,
                                  PublishTarget target, String taskId,
                                  AtomicInteger batchSeq, boolean isLastChunkOfMongoPage) {

        Map<String, Object> probeMessage = buildMessage(totalCount, records, batchSeq.get(), false);
        byte[] serialized = serializeQuietly(probeMessage);

        if (serialized.length <= MAX_PAYLOAD_BYTES || records.size() == 1) {
            if (serialized.length > MAX_PAYLOAD_BYTES) {
                // Single record still exceeds limit - log loudly, can't split further
                log.error("Single record exceeds Kafka size limit ({} bytes) for taskId {} - " +
                        "sending anyway, will likely be rejected by the broker", serialized.length, taskId);
            }
            int seq = batchSeq.getAndIncrement();
            Map<String, Object> finalMessage = buildMessage(totalCount, records, seq, isLastChunkOfMongoPage);
            log.info("Publishing batch seq {} - {} records, {} bytes", seq, records.size(), serialized.length);
            publish(finalMessage, target, taskId);
            return;
        }

        // Too big - split in half and recurse on each half
        int mid = records.size() / 2;
        List<OutagecardStatusInfo> firstHalf = records.subList(0, mid);
        List<OutagecardStatusInfo> secondHalf = records.subList(mid, records.size());

        log.info("Batch of {} records ({} bytes) exceeds {} byte limit - splitting into {} and {}",
                records.size(), serialized.length, MAX_PAYLOAD_BYTES, firstHalf.size(), secondHalf.size());

        splitAndPublish(firstHalf, totalCount, target, taskId, batchSeq, false);
        splitAndPublish(secondHalf, totalCount, target, taskId, batchSeq, isLastChunkOfMongoPage);
    }

    // ------------------------------------------------------------------
    // Message construction
    // ------------------------------------------------------------------

    private Map<String, Object> buildMessage(long totalCount, List<OutagecardStatusInfo> data,
                                              int batchSeq, boolean isLastBatch) {
        Map<String, Object> header = new LinkedHashMap<>();
        header.put("verb", "created");
        header.put("messageId", UUID.randomUUID().toString());

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("totalOutageCount", totalCount);
        payload.put("batchSeq", batchSeq);
        payload.put("isLastBatch", isLastBatch);
        payload.put("batchSize", data.size());
        payload.put("outageInfoList", data);

        Map<String, Object> message = new LinkedHashMap<>();
        message.put("header", header);
        message.put("payload", payload);
        return message;
    }

    private byte[] serializeQuietly(Map<String, Object> message) {
        try {
            return objectMapper.writeValueAsBytes(message);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize message for size check: {}", message, e);
            throw new RuntimeException("Serialization failed", e);
        }
    }

    // ------------------------------------------------------------------
    // Publishing
    // ------------------------------------------------------------------

    private void publish(Map<String, Object> message, PublishTarget target, String taskId) {
        switch (target) {
            case KAFKA -> publishToKafka(message, taskId);
            case REST_API -> callApi(message);
            default -> throw new IllegalArgumentException("Unsupported publish target: " + target);
        }
    }

    @SuppressWarnings("unchecked")
    private void publishToKafka(Map<String, Object> message, String taskId) {
        try {
            String jsonPayload = objectMapper.writeValueAsString(message);
            Map<String, Object> header = (Map<String, Object>) message.get("header");
            Map<String, Object> payload = (Map<String, Object>) message.get("payload");

            log.info("Publishing to Kafka - key: {}, messageId: {}, batchSeq: {}, size: {} bytes",
                    taskId, header.get("messageId"), payload.get("batchSeq"),
                    jsonPayload.getBytes(StandardCharsets.UTF_8).length);

            kafkaTemplate.send(KAFKA_TOPIC, taskId, jsonPayload)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Kafka publish failed - messageId: {}", header.get("messageId"), ex);
                        } else {
                            log.info("Kafka publish succeeded - messageId: {}, partition: {}, offset: {}",
                                    header.get("messageId"),
                                    result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset());
                        }
                    });
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize message for Kafka: {}", message, e);
            throw new RuntimeException("Kafka serialization failed", e);
        }
    }

    private void callApi(Map<String, Object> message) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(message, headers);

        try {
            ResponseEntity<String> response = restTemplate.exchange(
                    TARGET_API_URL, HttpMethod.POST, entity, String.class);
            log.info("API response: {}", response.getBody());
        } catch (RestClientException e) {
            log.error("API call failed: {}", message, e);
            throw e;
        }
    }
}
