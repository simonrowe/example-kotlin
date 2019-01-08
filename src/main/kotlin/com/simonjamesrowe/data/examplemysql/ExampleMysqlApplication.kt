package com.simonjamesrowe.data.examplemysql

import com.github.javafaker.Faker
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.Input
import org.springframework.cloud.stream.annotation.Output
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.context.annotation.Bean
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.repository.CrudRepository
import org.springframework.http.HttpStatus
import org.springframework.messaging.Message
import org.springframework.messaging.MessageChannel
import org.springframework.messaging.SubscribableChannel
import org.springframework.messaging.handler.annotation.SendTo
import org.springframework.messaging.support.MessageBuilder
import org.springframework.web.bind.annotation.*
import javax.persistence.Entity
import javax.persistence.GeneratedValue
import javax.persistence.GenerationType
import javax.persistence.Id

@SpringBootApplication
class ExampleMysqlApplication {

	@Bean
	fun redisTemplate(connectionFactory: RedisConnectionFactory) = StringRedisTemplate(connectionFactory)
}

fun main(args: Array<String>) {
	runApplication<ExampleMysqlApplication>(*args)
}


@Entity
data class Something(@Id @GeneratedValue(strategy = GenerationType.IDENTITY) var id: Long?,
					 var name: String)


interface SomethingRepository: CrudRepository<Something, Long>

@RestController
class Controller {

	@Autowired
	lateinit var somethingRepository: SomethingRepository

	@Autowired
	lateinit var stringRedisTemplate: StringRedisTemplate

	@Autowired
	lateinit var myMessageHandler: MyMessageHandler

	@PostMapping("/loadData")
	fun populateData(@RequestParam("numberOfItems") numberOfItems: Int) : String {
		val faker = Faker()
		for (i in 1 .. numberOfItems) somethingRepository.save(Something(null, faker.chuckNorris().fact()))
		return "OK"
	}

	@PostMapping("/loadCache")
	@ResponseStatus(HttpStatus.CREATED)
	fun populateCache(@RequestParam key: String, @RequestParam value: String) = stringRedisTemplate.opsForValue().set(key,value)

	@GetMapping("/getCache")
	fun getCacheVal(@RequestParam key: String): String? = stringRedisTemplate.opsForValue().get(key)

	@PostMapping("/sendMessage")
	fun sendMessage(@RequestBody value: String) = myMessageHandler.sendForProcessing(value)

}

@EnableBinding(MyMessageChannels::class)
class MyMessageHandler {

	private val logger = LoggerFactory.getLogger(javaClass)

	@Autowired
	@Qualifier("output")
	lateinit var output: MessageChannel

	fun sendForProcessing(value: String) {
		logger.info("Value passed through is ${value}")
		output.send(MessageBuilder.withPayload(value).setHeader("rawValue", value).build());
	}


	@StreamListener("processorInput")
	@SendTo("processorOutput" )
	fun process(value: String) = value.reversed();

	@StreamListener("sinkInput")
	fun sink(message : Message<String>) = logger.info("Message is ${message.payload}, Raw Value is ${message.headers["rawValue"]}")

}


interface MyMessageChannels {

	@Output("output")
	fun source() : MessageChannel

	@Input("processorInput")
	fun processorInput() : SubscribableChannel

	@Output("processorOutput")
	fun processorOutput() : MessageChannel

	@Input("sinkInput")
	fun sink() : SubscribableChannel

}


