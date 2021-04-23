package com.kafka.training.libraryeventsproducer.Producer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.training.libraryeventsproducer.Domain.Book;
import com.kafka.training.libraryeventsproducer.Domain.LibraryEvent;

@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerUnitTest {

	@InjectMocks
	private LibraryEventProducer libraryEventProducer;

	@Mock
	private KafkaTemplate<Integer, String> kafkaTemplate;

	@Spy
	private ObjectMapper objectMapper;

	@Test
	public void sendLibraryEvent_Approach2_failure()
			throws JsonProcessingException, InterruptedException, ExecutionException {
		// given
		Book book = Book.builder().bookId(123).bookAuthor("Ruhan").bookName("Integration Tests for kafka").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();

		SettableListenableFuture<SendResult<Integer, String>> future = new SettableListenableFuture<SendResult<Integer, String>>();
		future.setException(new RuntimeException("Exception calling Kafka"));
		when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);
		// when
		assertThrows(Exception.class, () -> libraryEventProducer.sendLibraryEventApproach2(libraryEvent).get());
	}

	@Test
	public void sendLibraryEvent_Approach2_success()
			throws JsonProcessingException, InterruptedException, ExecutionException {
		// given
		Book book = Book.builder().bookId(123).bookAuthor("Ruhan").bookName("Integration Tests for kafka").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();

		SettableListenableFuture<SendResult<Integer, String>> future = new SettableListenableFuture<SendResult<Integer, String>>();
		String record = objectMapper.writeValueAsString(libraryEvent);
		ProducerRecord<Integer, String> producerRecord = new ProducerRecord<Integer, String>("library-events",
				libraryEvent.getLibraryEventId(), record);
		RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-events", 1), 1, 1, 342,
				System.currentTimeMillis(), 1, 2);
		SendResult<Integer, String> sendResult = new SendResult<Integer, String>(producerRecord, recordMetadata);

		future.set(sendResult);
		when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);
		// when
		ListenableFuture<SendResult<Integer, String>> listenableFuture = libraryEventProducer
				.sendLibraryEventApproach2(libraryEvent);
		// then
		SendResult<Integer, String> sendResult2 = listenableFuture.get();
		assert sendResult2.getRecordMetadata().partition() == 1;
	}

}
