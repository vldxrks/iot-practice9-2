# Практична робота 9.2 - IoT Edge Computing + Cloud (варіант 1: Smart Factory Monitoring)

Дисципліна: Технології IoT та SMART-технології. Студент: Влад Рукасов, група Ік-42.

## Запуск
```
docker-compose up --build -d
docker-compose ps
docker logs -f edge-processor          # обробка та алерти на Edge
curl localhost:5000/api/dashboard/summary
# Grafana: http://localhost:3000 (admin/admin), дашборд "Smart Factory: Edge + Cloud"
# Тест автономії: docker-compose stop cloud-connector ... docker-compose start cloud-connector
```

## Мета
Розробити розподілену IoT-систему моніторингу виробничих ліній, у якій критичні рішення приймаються локально на Edge, а довготривале зберігання, аналітика й візуалізація виконуються в хмарі. Усі компоненти працюють у Docker-контейнерах.

## Архітектура
```
Sensors → MQTT Broker → Edge Processor → Cloud Connector → InfluxDB → Grafana
                              ↓
                      Actuators / SQLite buffer
```

| Сервіс | Технологія | Роль |
|---|---|---|
| sensor-simulator | Python, paho-mqtt | 5 датчиків на 2 лінії, випадкові несправності |
| mqtt-broker | Eclipse Mosquitto | Обмін повідомленнями, статус хмари (retained + Last Will) |
| edge-processor | Python | Правила, виявлення аномалій, агрегація, офлайн-буфер |
| influxdb | InfluxDB 2.7 | Bucket `edge` (7 днів) і `cloud` (довготривалий) |
| cloud-connector | Python, Flask | Прийом агрегатів і алертів, REST API, дедуплікація |
| grafana | Grafana 10 | Дашборд «Smart Factory: Edge + Cloud» |

## Що реалізовано
- **Edge:** порогові правила (warning/critical), EWMA-детектор аномалій (z-score), автоматичні команди виконавцям (наприклад, `enable_cooling`), 10-секундна агрегація, запис в InfluxDB пакетами.
- **Відмовостійкість:** коли хмара недоступна, Edge продовжує працювати й складає повідомлення в SQLite-буфер із унікальними id; після відновлення зв'язку відправляє їх пакетами. Хмара відкидає дублікати.
- **Cloud:** REST API (`/api/health`, `/api/sensors/<line>/<sensor>/data`, `/api/dashboard/summary`) і віддалена команда `POST /api/commands` для зміни порогів на Edge.
- **Візуалізація:** Grafana з автоналаштуванням джерел даних і дашборду.

## Висновки
Розподіл обчислень між Edge і Cloud дозволяє швидко реагувати на несправності локально та не втрачати дані при зникненні зв'язку, а хмара забезпечує історію й централізоване керування. [Допишіть 1-2 речення власних висновків: що було найскладнішим і що б ви покращили.]
## Декларація використання ШІ
ШІ Claude Перевірив логіку симулятора: переглянув графіки, переконався, що добова та тижнева сезонність видима і що датчики корельовані через навантаження.
Також допоміг з написанням файлу README та сценарію для відео звіту.
