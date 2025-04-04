# WebSub - Hub

## Commands

Check the valid messages the subscriber client has received:

```bash
curl http://localhost:8081/log
```

Re-subscribe (initiates the subscripton process), useful when developing:

```bash
curl http://localhost:8081/resub
```

Publish data to the hub:

```bash
curl -X POST http://localhost:8080/publish -H "Content-Type: application/json" -d '{"topic": "/a/topic", "content": {"message": "Hello World", "timestamp": 1649012345}}'
```