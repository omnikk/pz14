# Практическое занятие №14 — Реализация очереди задач (producer–consumer)

**Студент:** Выборнов Олег Андреевич  
**Группа:** ЭФМО-02-25  
**Дисциплина:** Технологии индустриального программирования  
**Преподаватель:** Адышкин Сергей Сергеевич  

---

## Цель работы

Освоить построение очереди задач по модели producer–consumer с использованием RabbitMQ: организовать повторные попытки обработки (retries), настроить очередь проблемных сообщений (DLQ) и реализовать идемпотентную обработку сообщений по `message_id`.

---

## Архитектура

```
POST /v1/jobs/process-task
          │
          ▼
┌─────────────────┐  attempt=1   ┌─────────────────────┐
│  tasks-service  │─────────────►│  RabbitMQ           │
│   (producer)    │              │  queue: task_jobs   │
└─────────────────┘              └──────────┬──────────┘
                                            │
                                            ▼
                                   ┌─────────────────┐
                                   │     worker      │
                                   │   (consumer)    │
                                   └────────┬────────┘
                                       success│ error (attempt <= 3)
                                            ack │retry → task_jobs (attempt++)
                                            │
                                     error (attempt > 3)
                                            │
                                            ▼
                                   ┌─────────────────────┐
                                   │  task_jobs_dlq      │
                                   │  (Dead Letter Queue)│
                                   └─────────────────────┘
```

---

## Структура проекта

```
pz14/
├── go.mod
├── deploy/
│   └── rabbit/
│       └── docker-compose.yml
├── services/
│   ├── tasks/                          — producer
│   │   ├── cmd/tasks/main.go           — HTTP + job enqueue
│   │   └── internal/
│   │       ├── amqp/publisher.go       — публикация в очередь
│   │       └── jobs/job.go             — формат сообщения
│   └── worker/                         — consumer
│       ├── cmd/worker/main.go          — логика обработки + retry + DLQ
│       └── internal/
│           └── store/processed.go      — idempotency store
├── images/
└── README.md
```

---

## Формат сообщения

```json
{
  "job":        "process_task",
  "task_id":    "t_001",
  "attempt":    1,
  "message_id": "msg-7234916482"
}
```

- `job` — тип задачи
- `task_id` — ID бизнес-объекта
- `attempt` — номер текущей попытки (начинается с 1)
- `message_id` — уникальный ID для идемпотентности

---

## Запуск

### 1. Запустить RabbitMQ

```powershell
cd deploy\rabbit
docker compose up -d
```

### 2. Запустить worker (первый терминал)

```powershell
cd services\worker
$env:RABBIT_URL="amqp://guest:guest@localhost:5672/"
go run ./cmd/worker
```

### 3. Запустить tasks-сервис (второй терминал)

```powershell
cd services\tasks
$env:RABBIT_URL="amqp://guest:guest@localhost:5672/"
go run ./cmd/tasks
```

---

## Проверки

### Успешная обработка

```powershell
curl -i -X POST http://localhost:8082/v1/jobs/process-task `
  -H "Content-Type: application/json" `
  -d '{"task_id":"t_001"}'
```

**Ответ `202 Accepted`:**
```json
{ "status": "accepted", "task_id": "t_001" }
```

**Лог worker:**
```
received: job=process_task task_id=t_001 attempt=1 message_id=msg-7234916482
  → SUCCESS: task_id=t_001 processed on attempt=1
```

![Успешная обработка](images/01_success.png)

---

### Ошибочная обработка (retries → DLQ)

```powershell
curl -i -X POST http://localhost:8082/v1/jobs/process-task `
  -H "Content-Type: application/json" `
  -d '{"task_id":"t_fail"}'
```

**Лог worker (3 попытки → DLQ):**
```
received: job=process_task task_id=t_fail attempt=1 message_id=msg-9182736450
  → ERROR: attempt=1/3: simulated processing error for task_id=t_fail
  → RETRY: re-queued as attempt=2
received: job=process_task task_id=t_fail attempt=2 message_id=msg-9182736450
  → ERROR: attempt=2/3: simulated processing error for task_id=t_fail
  → RETRY: re-queued as attempt=3
received: job=process_task task_id=t_fail attempt=3 message_id=msg-9182736450
  → ERROR: attempt=3/3: simulated processing error for task_id=t_fail
  → DLQ: message_id=msg-9182736450 moved to task_jobs_dlq after 3 attempts
```

![Retries и DLQ](images/02_dlq.png)

---

### Проверка DLQ в Management UI

Открыть [http://localhost:15672](http://localhost:15672) → Queues. Очередь `task_jobs_dlq` содержит проблемные сообщения.

![DLQ в Management UI](images/03_ui_dlq.png)

---

### Идемпотентность: повторная доставка одного message_id

При повторной доставке сообщения с тем же `message_id` worker пропускает обработку:

```
received: job=process_task task_id=t_001 attempt=1 message_id=msg-7234916482
  → SKIP: message_id=msg-7234916482 already processed (idempotent)
```

![Идемпотентная обработка](images/04_idempotent.png)

---

## Ключевые фрагменты реализации

### Логика retry и DLQ (worker)

```go
const maxAttempts = 3

if err := processTask(job); err != nil {
    job.Attempt++
    if job.Attempt <= maxAttempts {
        publishJob(ch, "task_jobs", job)   // retry: сообщение с увеличенным attempt
        d.Ack(false)                        // исходное подтверждаем
        return
    }
    publishJob(ch, "task_jobs_dlq", job)   // исчерпаны попытки → DLQ
    d.Ack(false)
    return
}
```

Важно: исходное сообщение всегда подтверждается (`Ack`). Если оставить без подтверждения, RabbitMQ повторит его автоматически, что создаст неконтролируемый цикл.

### Идемпотентность

```go
if processed.Exists(job.MessageID) {
    d.Ack(false)  // уже обработано — просто подтверждаем
    return
}
// ... обработка ...
processed.MarkDone(job.MessageID)
```

### Имитация ошибки

```go
func processTask(job TaskJob) error {
    time.Sleep(500 * time.Millisecond)
    if job.TaskID == "t_fail" {
        return fmt.Errorf("simulated processing error")
    }
    return nil
}
```

---

## Контрольные вопросы

**1. Чем задача в очереди отличается от простого события?**

Событие — короткое уведомление о произошедшем (fire-and-forget). Задача — запрос на выполнение работы, которая может занять время, завершиться ошибкой и потребовать повторной попытки. Именно поэтому для задач появляются retries, DLQ и идемпотентность.

**2. Зачем нужны retries?**

Некоторые ошибки временны: сервис недоступен, соединение оборвалось, ресурс занят. Повторная попытка через некоторое время может завершиться успешно. Без retries такие задачи терялись бы при первой ошибке.

**3. Почему нельзя бесконечно возвращать ошибочное сообщение в основную очередь?**

Сообщение с неисправимой ошибкой будет бесконечно занимать ресурсы и мешать обработке нормальных задач. Ограниченное число попыток + DLQ — стандартный паттерн.

**4. Что такое DLQ и зачем она используется?**

Dead Letter Queue — очередь для сообщений, которые не удалось обработать успешно за отведённое число попыток. DLQ позволяет: не терять сообщения, не блокировать основную очередь, сохранить возможность ручного разбора.

**5. Почему в системах очередей возможна повторная доставка?**

RabbitMQ (и большинство брокеров) работают в режиме at-least-once delivery. Если consumer не успел отправить ack до сбоя, брокер считает сообщение необработанным и повторит доставку.

**6. Что такое идемпотентность обработчика?**

Идемпотентный обработчик даёт одинаковый результат при повторном выполнении с теми же входными данными. В данной работе: если то же `message_id` приходит повторно, обработчик пропускает работу и только подтверждает сообщение.

**7. Зачем нужен message_id?**

`message_id` — уникальный идентификатор конкретного сообщения. Он позволяет worker'у определить, обрабатывалось ли это сообщение ранее, независимо от содержимого (task_id, attempt и т.д.).

**8. Почему хранение обработанных message_id в памяти полезно для учебного примера?**

Для демонстрации принципа идемпотентности этого достаточно. В production использовался бы Redis или Postgres — чтобы обработанные ID не терялись при перезапуске worker'а.

**9. Что произойдёт, если worker выполнит обработку, но не успеет отправить ack?**

RabbitMQ повторит доставку того же сообщения. Если обработчик идемпотентен (проверяет message_id), повторное выполнение будет безвредным — сообщение будет пропущено и подтверждено.

**10. Почему модель producer–consumer удобна для тяжёлых фоновых задач?**

Producer (HTTP-сервис) сразу отвечает клиенту, не дожидаясь завершения тяжёлой работы. Consumer обрабатывает задачи в своём темпе. Оба компонента масштабируются независимо. При сбое worker'а задачи остаются в очереди и будут обработаны после его перезапуска.
