package com.kafka.training.libraryeventsproducer.Controllers;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.training.libraryeventsproducer.Domain.Book;
import com.kafka.training.libraryeventsproducer.Domain.LibraryEvent;
import com.kafka.training.libraryeventsproducer.Producer.LibraryEventProducer;

@WebMvcTest(LibraryEventsController.class)
@AutoConfigureMockMvc
public class LibraryEventControllerUnitTest {

	@Autowired
	private MockMvc mockMvc;

	ObjectMapper objectMapper = new ObjectMapper();

	@MockBean
	private LibraryEventProducer libraryEventProducer;

	@Test
	public void postLibraryEvent() throws Exception {
		// given
		Book book = Book.builder().bookId(123).bookAuthor("Ruhan").bookName("Integration Tests for kafka").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();
		String jsonString = objectMapper.writeValueAsString(libraryEvent);
		// when
		when(libraryEventProducer.sendLibraryEventApproach2(libraryEvent)).thenReturn(null);
		mockMvc.perform(post("/library/v1/book").content(jsonString).contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isCreated());

	}

	@Test
	public void postLibraryEvent_4xx() throws Exception {
		// given
		Book book = Book.builder().bookId(null).bookAuthor(null).bookName("Integration Tests for kafka").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();
		String jsonString = objectMapper.writeValueAsString(libraryEvent);
		// when
		when(libraryEventProducer.sendLibraryEventApproach2(libraryEvent)).thenReturn(null);
		String expectedErrorMessage = "book.bookAuthor - must not be blank, book.bookId - must not be null";
		mockMvc.perform(post("/library/v1/book").content(jsonString).contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().is4xxClientError()).andExpect(content().string(expectedErrorMessage));
	}

}
