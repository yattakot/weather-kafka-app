# Приложение: Анализ погоды для путешествий 🌤️

Реализовано:
- Продюсер Kafka: генерирует погоду в разных городах
- Консьюмер: собирает статистику (дождь, жара, грибы)
- Симуляция недели
- Автоотчёт

## Как запустить

1. `docker-compose up`
2. Запустить `WeatherConsumer`
3. Запустить `WeatherProducer`

Через 60 секунд — финальный отчёт!
