# StreamTask

StreamTask 是一个基于 Redis Stream 的轻量级可靠异步任务组件，通过 Spring Boot Starter 提供任务发布、消费者组消费、手动确认、失败重试、死信队列、Pending 恢复、幂等保护和基础可观测性。

## 可靠性语义

StreamTask 提供至少一次投递语义。任务成功后执行 `XACK`；任务失败或 Worker 异常退出时，消息留在 PEL 中，恢复线程通过 `XAUTOCLAIM` 接管。组件不承诺跨 Redis 与业务数据库的严格 Exactly-Once，业务侧仍应使用唯一索引、状态机或幂等写入兜底。

## 模块

- `stream-task-core`：核心 API、发布、消费、恢复、DLQ、幂等和管理服务。
- `stream-task-spring-boot-autoconfigure`：Spring Boot 自动配置。
- `stream-task-spring-boot-starter`：业务方引入的 Starter。
- `stream-task-demo`：可运行示例。

## 快速开始

启动 Redis：

```bash
docker compose up -d redis
```

启动 Demo：

```bash
mvn -pl stream-task-demo spring-boot:run
```

发布任务：

```bash
curl -X POST http://localhost:8091/demo/tasks/success
curl -X POST http://localhost:8091/demo/tasks/fail
curl -X POST http://localhost:8091/demo/tasks/slow
```

查看管理接口：

```bash
curl http://localhost:8091/stream-task/admin/overview
curl http://localhost:8091/stream-task/admin/pending
curl http://localhost:8091/stream-task/admin/dlq
```

也可以使用 `scripts` 目录下的演示脚本：

```bash
./scripts/publish-demo-task.sh success
./scripts/publish-demo-task.sh fail
./scripts/inspect-pending.sh
./scripts/replay-dlq.sh <dlq-message-id>
```

## 接入方式

```xml
<dependency>
    <groupId>cn.heycloudream</groupId>
    <artifactId>stream-task-spring-boot-starter</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

```yaml
stream-task:
  enabled: true
  namespace: demo
  group: stream-task-demo-group
  admin:
    enabled: false
```

发布任务：

```java
streamTaskTemplate.publish("document.parse", "task-10001", request);
```

编写处理器：

```java
@Component
public class DocumentParseHandler implements StreamTaskHandler {
    public String taskType() {
        return "document.parse";
    }

    public void handle(StreamTaskEnvelope task) {
        // business logic
    }
}
```

## 测试

```bash
mvn clean test
```

## 已知限制

- 第一版面向单 Redis 实例或 Sentinel 场景。
- 第一版使用固定间隔恢复，不实现指数退避。
- Redis Stream 不作为永久审计库。
- 管理接口默认关闭，生产启用时应通过网关或 Spring Security 做保护。
