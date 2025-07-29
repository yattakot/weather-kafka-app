package org.example.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor  // 👈 нужен для Jackson
@AllArgsConstructor // 👈 чтобы можно было создавать с параметрами
public class WeatherData {
    private String city;
    private double temperature;
    private String condition;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm")
    private LocalDateTime timestamp;

    @Override
    public String toString() {
        return String.format("Город: %s, Темп: %.1f°C, Условие: %s, Дата: %s",
                city, temperature, condition, timestamp);
    }
}