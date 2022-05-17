package com.credit.suisse.logevents;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

 
class LogeventsApplicationTests {

	@Test
	void testlogEvents() throws IOException, InterruptedException, ExecutionException {
		
		ClassLoader classLoader = getClass().getClassLoader();
		File file = new File(classLoader.getResource("demo2.txt").getFile());
		
		 
		
		assertEquals(10,LogEventSerivce.logEvents(file.getAbsolutePath()).size()) ;
	}

}
