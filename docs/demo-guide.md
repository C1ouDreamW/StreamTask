# Demo 指南

## 启动

```bash
docker compose up -d redis
mvn -pl stream-task-demo spring-boot:run
```

Demo 默认端口为 `8091`。

## 正常任务

```bash
curl -X POST http://localhost:8091/demo/tasks/success
curl http://localhost:8091/stream-task/admin/overview
```

预期：任务被消费并 ACK，Pending 数为 0。

## 失败与死信

```bash
curl -X POST http://localhost:8091/demo/tasks/fail
curl http://localhost:8091/stream-task/admin/pending
curl http://localhost:8091/stream-task/admin/dlq
```

预期：任务失败后先留在 PEL，恢复线程重试，超过 `max-attempts` 后进入 DLQ。

## 慢任务

```bash
curl -X POST http://localhost:8091/demo/tasks/slow
```

预期：任务执行 90 秒。幂等租约由 Watchdog 续期，避免长任务执行中租约过期。

## DLQ 重放

先从 `/stream-task/admin/dlq` 中取得死信消息 ID，再执行：

```bash
curl -X POST http://localhost:8091/stream-task/admin/dlq/{messageId}/replay
```
