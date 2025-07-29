// org/example/producer/WeatherProducer.java
package org.example.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.model.WeatherData;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.Random;

public class WeatherProducer {
    private static final String TOPIC = "weather-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    private static final String[] CITIES = {
            "Москва", "Санкт-Петербург", "Новосибирск", "Екатеринбург",
            "Магадан", "Чукотка", "Тюмень"
    };

    private static final String[] CONDITIONS = {"солнечно", "облачно", "дождь"};
    private static final double MAX_TEMP = 35.0;
    private static final LocalDateTime SIMULATION_START = LocalDateTime.of(2024, 7, 1, 0, 0);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Random random = new Random();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n[Producer] Завершение работы...");
            producer.close();
        }));

        System.out.println("[Producer] Запущен. Отправка данных о погоде каждые 2 секунды...");

        try {
            while (!Thread.currentThread().isInterrupted()) {
                // Случайный город
                String city = CITIES[random.nextInt(CITIES.length)];

                // Случайная температура от 0 до 35
                double temperature = Math.round(random.nextDouble() * MAX_TEMP * 10) / 10.0;

                // Случайное состояние
                String condition = CONDITIONS[random.nextInt(CONDITIONS.length)];

                // Случайная дата в пределах недели (7 дней)
                int daysOffset = random.nextInt(7);
                LocalDateTime eventTime = SIMULATION_START.plusDays(daysOffset);

                WeatherData weatherData = new WeatherData(city, temperature, condition, eventTime);

                String json = objectMapper.writeValueAsString(weatherData);
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, city, json);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Ошибка отправки: " + exception.getMessage());
                    } else {
                        System.out.printf("✅ Отправлено: %s (partition: %d, offset: %d)%n",
                                json, metadata.partition(), metadata.offset());
                    }
                });

                Thread.sleep(2000); // каждые 2 секунды
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("[Producer] Прерван.");
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        } finally {
            producer.close();
        }
    }
}