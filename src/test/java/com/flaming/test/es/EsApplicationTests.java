package com.flaming.test.es;

import com.flaming.test.es.service.EsService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class EsApplicationTests {

	@Autowired
	private EsService esService;

	@Test
	public void test(){
		esService.esServiceSearch("测试");
	}
}
