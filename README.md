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

## Декларація використання ШІ
ШІ Claude Перевірив логіку симулятора: переглянув графіки, переконався, що добова та тижнева сезонність видима і що датчики корельовані через навантаження.
Також допоміг з написанням файлу README та сценарію для відео звіту.