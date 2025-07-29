// org/example/consumer/WeatherConsumer.java
package org.example.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.model.WeatherData;
import org.apache.kafka.common.errors.WakeupException;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class WeatherConsumer {
    private static final String TOPIC = "weather-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String GROUP_ID = "weather-consumer-group";

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    // Статистика
    private final Map<String, Integer> rainyDaysCount = new HashMap<>();
    private final Map<String, Double> maxTemperature = new HashMap<>();
    private final Map<String, List<Double>> cityTemperatures = new HashMap<>();
    private final List<LocalDateTime> tyumenRainyDays = new ArrayList<>();

    private int messageCount = 0;
    private final int REPORT_AFTER_MESSAGES = 50;
    private final Timer timer = new Timer();

    public static void main(String[] args) {
        new WeatherConsumer().start();
    }

    public void start() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        // Таймер: завершить через 60 секунд
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                System.out.println("\n⏰ Время истекло. Генерация данных остановлена.");
                consumer.wakeup();
            }
        }, 60_000);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n[Consumer] Завершение работы...");
            timer.cancel();
            consumer.close();
            printFinalReport();
        }));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        WeatherData wd = objectMapper.readValue(record.value(), WeatherData.class);
                        processWeatherData(wd);
                        messageCount++;

                        if (messageCount % REPORT_AFTER_MESSAGES == 0) {
                            System.out.printf("\n📊 Промежуточный отчёт (после %d сообщений):\n", messageCount);
                            printPartialReport();
                        }
                    } catch (Exception e) {
                        System.err.println("Ошибка парсинга: " + e.getMessage());
                    }
                }
            }
        } catch (WakeupException e) {
            // нормально — прервано таймером
        } finally {
            consumer.close();
            printFinalReport();
        }
    }

    private void processWeatherData(WeatherData wd) {
        String city = wd.getCity();

        rainyDaysCount.merge(city, "дождь".equals(wd.getCondition()) ? 1 : 0, Integer::sum);
        maxTemperature.merge(city, wd.getTemperature(), Math::max);
        cityTemperatures.computeIfAbsent(city, k -> new ArrayList<>()).add(wd.getTemperature());

        if ("Тюмень".equals(city) && "дождь".equals(wd.getCondition())) {
            tyumenRainyDays.add(wd.getTimestamp());
        }
    }

    private void printPartialReport() {
        System.out.println("  Самая жаркая погода:");
        maxTemperature.forEach((city, temp) ->
                System.out.printf("    %s: %.1f°C%n", city, temp));

        System.out.println("  Дождливые дни:");
        rainyDaysCount.forEach((city, count) ->
                System.out.printf("    %s: %d дней%n", city, count));
    }

    private void printFinalReport() {
        System.out.println("\n" + new String(new char[50]).replace('\0', '='));
        System.out.println("            🌤️ ФИНАЛЬНЫЙ ОТЧЁТ ПО ПОГОДЕ");
        System.out.println(new String(new char[50]).replace('\0', '='));

        System.out.printf("Обработано сообщений: %d%n", messageCount);

        System.out.println("\n🔥 Самая высокая температура:");
        maxTemperature.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .forEach(e -> System.out.printf("  %s: %.1f°C%n", e.getKey(), e.getValue()));

        System.out.println("\n🌧️ Количество дождливых дней:");
        rainyDaysCount.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .forEach(e -> System.out.printf("  %s: %d дней%n", e.getKey(), e.getValue()));

        System.out.println("\n🌡️ Средняя температура:");
        cityTemperatures.forEach((city, temps) -> {
            double avg = temps.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
            System.out.printf("  %s: %.1f°C%n", city, avg);
        });

        // Грибы в Тюмени — если 2+ дня подряд дождь
        Set<LocalDateTime> rainyDates = tyumenRainyDays.stream()
                .map(dt -> dt.toLocalDate().atStartOfDay())
                .collect(Collectors.toSet());

        List<LocalDateTime> sorted = tyumenRainyDays.stream()
                .map(dt -> dt.toLocalDate().atStartOfDay())
                .distinct()
                .sorted()
                .collect(Collectors.toList()); // ✅ Вместо .toList()

        boolean hasConsecutiveRain = false;
        for (int i = 1; i < sorted.size(); i++) {
            if (sorted.get(i).toLocalDate().minusDays(1).equals(sorted.get(i - 1).toLocalDate())) {
                hasConsecutiveRain = true;
                break;
            }
        }

        System.out.println("\n🍄 Можно идти за грибами в Тюмени? " +
                (hasConsecutiveRain ? "✅ Да, были подряд дождливые дни!" : "❌ Нет, дождей недостаточно."));

        System.out.println(new String(new char[50]).replace('\0', '='));
    }
}