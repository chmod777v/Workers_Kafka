<h1 align="center">
    💻 Workers_Kafka 🎞️
</h1>



## 🗒️ Описание
Асинхронная распределённая система обработки задач: клиент ставит задачу
через HTTP-шлюз, воркеры обрабатывают её в фоне, результат забирается
опросом по токену. Построена на Kafka, PostgreSQL и Redis, разворачивается
в Kubernetes.

> В коде «задача» — это простая генерация сообщения: worker [рандомно выбирает строку из массива, искусственно ждёт (имитация тяжёлой работы)](https://github.com/chmod777v/Workers_Kafka/blob/main/worker/internal/worker/worker.go) и публикует его в `result`. 
> Цель примера — показать поток: запрос → очередь → результат, а не конкретную бизнес-логику.



## ❔ Зачем это
Демонстрирует паттерн [async request-reply](https://learn.microsoft.com/azure/architecture/patterns/asynchronous-request-reply):

- Шлюз быстро отвечает `202 Accepted` и не блокируется на обработке;
- Тяжёлая работа выполняется воркерами, которые масштабируются горизонтально (в Kubernetes — числом реплик);
- Ответ на GET кэшируется в Redis, источник истины — БД;
- Всё наблюдаемо: метрики в Prometheus и Grafana, логи в Loki;



## Архитектура
```mermaid
flowchart LR
    C[Клиент] -->|POST /task| G[gateway]
    G -->|produce| T1[(Kafka: tasks)]
    T1 -->|consume| W[worker × N]
    W -->|produce| T2[(Kafka: result)]
    T2 -->|consume async| G
    G --> DB[(PostgreSQL<br/>источник истины)]
    G --> RD[(Redis<br/>кэш)]
    C -->|"GET /task/{token}"| G
```
> На схеме Kafka показана как два логических топика (`tasks` и `result`), а worker — как роль. Кластер Kafka один, но состоять он может из нескольких брокеров. Воркеров может быть несколько(по задумке их много) — они масштабируются горизонтально.

### Компоненты
| Компонент | Роль |
|---|---|
| **gateway** | HTTP API: принимает `POST /task`, отвечает `202 Accepted` + токен; обслуживает `GET /task/{token}`; отдельным consumer'ом читает топик `result` и обновляет статус в БД |
| **worker** | Stateless-обработчик: читает `tasks`, выполняет задачу по `token` и публикует результат в `result`; масштабируется числом реплик |
| **Kafka** | Брокер сообщений: топик `tasks` (задачи) и `result` (результаты) |
| **PostgreSQL** | Источник истины: `token`, сообщение результата (если поле пустое => статус: "обрабатывается")|
| **Redis** | Кэш ответов GET, чтобы не бить в БД на каждом опросе |
| **Prometheus + Grafana** | Метрики и дашборды |
| **Loki + Promtail** | Сбор и просмотр логов |

### Как читать схему
- **POST-поток**: клиент → gateway → запись в БД → produce в `tasks` → `202 Accepted` с токеном. Gateway не ждёт обработки.
- **Обработка**: worker забирает задачу из `tasks`, выполняет, публикует `{token, message}` в `result`.
- **Завершение**: consumer в gateway читает `result` и обновляет сообщение в БД.
- **GET-поток**: клиент → gateway → Redis → (при промахе) PostgreSQL → ответ.
> Алгоритм работы также описан в [плане](https://github.com/chmod777v/Workers_Kafka/blob/main/plan.txt) проекта



## 🛡️ Гарантии доставки
Планируется обеспечить [exactly-once](https://habr.com/ru/companies/badoo/articles/333046/) (В процессе)



## 🛠 Технологии

### Компоненты 
| Компонент | Технология |
|---|---|
| **Язык** | `go 1.27.1`  |
| **HTTP-роутер** | `go-chi/chi/v5` |
| **Конфигурация** | `cleanenv` |
| **PostgreSQL-клиент** | `pgx/v5` |
| **Redis-клиент** | `go-redis/v9` |
| **Kafka-клиент** | `segmentio/kafka-go` |
| **Метрики** | `prometheus/client_golang` |
| **Логирование** | `slog` + `lmittmann/tint` |

### Инфраструктурные сервисы
| Слой | Технология | Образ |
|---|---|---|
| **Брокер сообщений** | Apache Kafka | `confluentinc/cp-kafka` |
| **UI для брокера сообщений** | Kafka UI | `provectuslabs/kafka-ui` |
| **База данных** | PostgreSQL | `postgres` |
| **Кэш** | Redis | `redis` |
| **Метрики** | Prometheus | `prom/prometheus` |
| **Дашборды** | Grafana | `grafana/grafana` |
| **Логи** | Loki | `grafana/loki` |
| **Сбор логов** | Promtail | `grafana/promtail` |