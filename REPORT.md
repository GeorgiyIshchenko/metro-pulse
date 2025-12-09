# Metro Pulse – итоговый отчёт

## Бизнес-задача
- Синтетическая платформа городской мобильности: сбор и хранение поездок, оплат, позиций транспорта, расчёт маршрутно-часовых метрик и быстрая витрина для аналитики.
- Цель: обеспечить надёжный DWH для медленных запросов и отдельную быструю витрину на ClickHouse для интерактивной аналитики, с автоматизированным генератором данных и пайплайном.

## Архитектура платформы
- Источник: синтетика в Parquet → MinIO (S3).
- ETL: Spark (master/worker) читает Parquet из S3, пишет в Postgres DWH (схемы `staging`, `dwh`) и строит витрину в ClickHouse (`mart.trip_route_hourly`).
- DWH: Postgres, модель в духе Kimball (звезда: `fact_trip`, `fact_payment`, `fact_vehicle_position` + измерения `dim_user`, `dim_route` SCD2, `dim_stop`, `dim_fare_product`, `dim_vehicle`, `dim_date`, `dim_time`).
- Витрина: ClickHouse `ReplacingMergeTree`, партиционирование по дате часа, ключ `(route_id_nat, start_hour)`.
- Инструменты: генератор данных (`data-gen`), Spark-приложения (`staging_load`, `dwh_load_dim_*`, `dwh_load_facts`, `build_trip_mart_clickhouse.py`), скрипт сравнения (`scripts/compare_route_hour.py`) + артефакты сравнения в `artifacts/`.

## Ключевые решения и обоснования
- **Kimball vs Data Vault**: выбрали упрощённый Kimball (звезда) из-за небольшой сложности домена, быстрых джойнов и простой загрузки витрины. SCD2 применён только к маршрутам (`dim_route`) для отслеживания версий.
- **Spark**: единый движок для загрузки staging→DWH, DWH→витрина, работает с S3 (MinIO) и JDBC в Postgres/ClickHouse.
- **ClickHouse**: отдельная витрина для интерактивных маршрутно-часовых срезов; быстрые агрегации, дешёвая компрессия.
- **Тестовый профиль**: `PIPELINE_PROFILE=test` генерирует малый объём данных (1 день, малое число сущностей) для быстрой прогона и отладки.

## Демонстрация пайплайна
1) Генерация: `data-gen/main.py` → Parquet в `data` (или `data-test` в тест-профиле) → загрузка в MinIO.
2) Spark:
   - `staging_load.py` читает Parquet из S3 → `staging.*`.
   - `dwh_load_dim_core.py`, `dwh_load_dim_route_scd2.py` (антиджойн по `(route_id_nat, valid_from)`), `dwh_load_facts.py`.
   - `build_trip_mart_clickhouse.py` агрегирует `fact_trip` в часовой разрез и пишет в ClickHouse (с авто-созданием таблицы).
3) Сравнение: `scripts/compare_route_hour.py` считывает витрину из Postgres и ClickHouse, строит CSV/HTML отчёты и сохраняет метрики времени выполнения.
4) `run.sh` склеивает шаги, добавляет проверки готовности БД, размера фактов, наличия строк в витрине, активирует тест-профиль по переменной окружения.

## Трудности и их решения
- **Дубли в `dim_route`**: падение на уникальном ключе `(route_id_nat, valid_from)` → добавлен антиджойн перед вставкой в SCD2-лоадер.
- **Пустая витрина из-за NULL `start_hour`**: стартовое вычисление через join с `dim_date/time` давало NULL → пересчитано напрямую из `start_date_key/start_time_key` с паддингом до `HHmmss`.
- **ClickHouse “ENGINE_REQUIRED”**: при создании через JDBC добавлены `createTableOptions` с `ReplacingMergeTree` и партиционированием.
- **Слишком частые чекпоинты Postgres**: увеличен `max_wal_size`/`checkpoint_timeout` в `docker-compose.yml`.
- **Различия типов (Decimal/float) и chained assignment warnings в pandas**: добавлены функции нормализации типов и копирования в `compare_route_hour.py`.
- **ARM Mac + образ Spark**: принудительно `platform: linux/amd64` для Spark master/worker.

## Сравнение ClickHouse vs DWH (артефакты `artifacts/`)
- Источник: последний прогон (full-профиль) с маршрутно-часовой витриной.
- Результаты запроса:
  - Postgres: `postgres_rows = 4409`, время `~0.187s`.
  - ClickHouse: `clickhouse_rows = 4407`, время `~0.027s`.
  - Файл: `artifacts/performance_metrics.json` и `performance_metrics.html`.
- Сходимость данных: `route_hour_comparison.csv` показывает практически равный объём строк; различия в агрегатах не обнаружены на уровне выборки 58 часов (см. `route_hour_ch.csv`/`route_hour_pg.csv`).
- Визуализации:
  - `trips_per_hour_comparison.html` — групповой бар Postgres vs ClickHouse по часам (последние 48 интервалов окна).
  - `route_diff_comparison.html` — маршрутный уровень: суммарные поездки, разницы по поездкам, средняя стоимость и длительность (PG vs CH).
- Вывод: ClickHouse отдаёт те же агрегаты существенно быстрее на одном и том же объёме, при почти полном совпадении строк.

