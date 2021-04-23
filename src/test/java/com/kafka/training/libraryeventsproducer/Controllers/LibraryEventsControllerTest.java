package com.kafka.training.libraryeventsproducer.Controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import com.kafka.training.libraryeventsproducer.Domain.Book;
import com.kafka.training.libraryeventsproducer.Domain.LibraryEvent;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = { "library-events" }, partitions = 3)
@TestPropertySource(properties = { "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
		"spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}" })
class LibraryEventsControllerTest {

	@Autowired
	TestRestTemplate testRestTemplate;

	private Consumer<Integer, String> consumer;

	@Autowired
	private EmbeddedKafkaBroker embeddedKafkaBroker;

	@BeforeEach
	void setUp() {
		Map<String, Object> configs = new HashMap<>(
				KafkaTestUtils.consumerProps("group-1", "true", embeddedKafkaBroker));
		consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer())
				.createConsumer();
		embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
	}

	@AfterEach
	private void tearDown() {
		consumer.close();
	}

	@Test
	@Timeout(5)
	void postLibraryEvent() throws InterruptedException {
		// given
		Book book = Book.builder().bookId(123).bookAuthor("Ruhan").bookName("Integration Tests for kafka - Post Method").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();
		HttpHeaders headers = new HttpHeaders();
		headers.set("content-type", org.springframework.http.MediaType.APPLICATION_JSON.toString());
		HttpEntity<LibraryEvent> requestEntity = new HttpEntity<>(libraryEvent, headers);

		// when
		ResponseEntity<LibraryEvent> responseEntity = testRestTemplate.exchange("/library/v1/book", HttpMethod.POST,
				requestEntity, LibraryEvent.class);
		// then
		assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());
		ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, "library-events");
//		Thread.sleep(3000);
		String actualRecord = consumerRecord.value();
		String expectedRecord = "{\"libraryEventId\":null,\"libraryEventType\":\"New\",\"book\":{\"bookId\":123,\"bookName\":\"Integration Tests for kafka - Post Method\",\"bookAuthor\":\"Ruhan\"}}";
		assertEquals(expectedRecord, actualRecord);
	}

	@Test
	@Timeout(5)
	void putLibraryEvent() {
		Book book = Book.builder().bookId(123).bookAuthor("Ruhan").bookName("Integration Tests for kafka - Put Method").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(1).book(book).build();
		HttpHeaders headers = new HttpHeaders();
		headers.set("content-type", org.springframework.http.MediaType.APPLICATION_JSON.toString());
		HttpEntity<LibraryEvent> requestEntity = new HttpEntity<>(libraryEvent, headers);

		// when
		ResponseEntity<LibraryEvent> responseEntity = testRestTemplate.exchange("/library/v1/book", HttpMethod.PUT,
				requestEntity, LibraryEvent.class);
		// then
		assertEquals(HttpStatus.OK, responseEntity.getStatusCode());
		ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, "library-events");
//		Thread.sleep(3000);
		String actualRecord = consumerRecord.value();
		String expectedRecord = "{\"libraryEventId\":1,\"libraryEventType\":\"Update\",\"book\":{\"bookId\":123,\"bookName\":\"Integration Tests for kafka - Put Method\",\"bookAuthor\":\"Ruhan\"}}";
		assertEquals(expectedRecord, actualRecord);
	}

}
