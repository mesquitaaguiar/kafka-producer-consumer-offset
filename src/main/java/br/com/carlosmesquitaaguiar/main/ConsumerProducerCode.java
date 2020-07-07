package br.com.carlosmesquitaaguiar.main;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

public class ConsumerProducerCode {
	
	private static final KafkaConsumer<String, String> _consumer = new KafkaConsumer<String, String>(getPropertiesConsumer("localhost:9092", "all", "test"));
	private static final KafkaProducer<String, String> _producer = new KafkaProducer<String, String>(getPropertiesProducer("localhost:9092", "all"));
	
	private static final Logger logger = Logger.getLogger(ConsumerProducerCode.class);
	
	public static Properties getPropertiesConsumer(String server,String acks,String grupo) {
		Properties props = new Properties();
        props.put("bootstrap.servers", server); 
        props.put("acks", acks);
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "false");
        props.put("group.id", grupo);
        return props;
	}
	
	public static Properties getPropertiesProducer(String server,String acks) {
		Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", server);
        kafkaProps.put("acks", "all");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return kafkaProps;
	}
	
	public static String find(String cpf) {
		String retorno = "";
		switch (cpf) {
			case "55588899966":
				retorno = "{code:1,descricao:Encontrado}";
			break;
			default:
				retorno = "{code:2,descricao:Nao Encontrado}";
			break;
		}
		return retorno;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) {
		
		  _consumer.subscribe(Arrays.asList("test1"));
		  
		  while (true) { ConsumerRecords<String, String> records = _consumer.poll(100);
			for (ConsumerRecord<String, String> record : records){
				logger.debug("-----------------------------------------------------");
				logger.debug(record.value() +" - offset -> "+ record.offset());
				_producer.send(new ProducerRecord("test2", find(record.value()) ));
				_consumer.commitSync();
				logger.debug("-----------------------------------------------------");
			} 
		  }
	}
	
}
