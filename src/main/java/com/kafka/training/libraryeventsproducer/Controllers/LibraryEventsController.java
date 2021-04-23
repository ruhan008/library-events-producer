package com.kafka.training.libraryeventsproducer.Controllers;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafka.training.libraryeventsproducer.Domain.LibraryEvent;
import com.kafka.training.libraryeventsproducer.Domain.LibraryEventType;
import com.kafka.training.libraryeventsproducer.Producer.LibraryEventProducer;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
@RequestMapping("/library")
public class LibraryEventsController {

	@Autowired
	private LibraryEventProducer libraryEventsProducer;

	@PostMapping("/v1/book")
	public ResponseEntity<LibraryEvent> postBookEvent(@RequestBody @Valid LibraryEvent libraryEvent)
			throws JsonProcessingException, InterruptedException, ExecutionException, TimeoutException {
		libraryEvent.setLibraryEventType(LibraryEventType.New);
		log.info("postBookEvent() invoked with payload {}", libraryEvent);
		libraryEventsProducer.sendLibraryEventApproach2(libraryEvent);
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
	}

	@PutMapping("/v1/book")
	public ResponseEntity<?> putBookEvent(@RequestBody @Valid LibraryEvent libraryEvent)
			throws JsonProcessingException, InterruptedException, ExecutionException, TimeoutException {
		if (libraryEvent.getLibraryEventId() == null) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the libraryEventId");
		}
		libraryEvent.setLibraryEventType(LibraryEventType.Update);
		log.info("putBookEvent() invoked with payload {}", libraryEvent);
		libraryEventsProducer.sendLibraryEventApproach2(libraryEvent);
		return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
	}

}
