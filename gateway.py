from fastapi import FastAPI, HTTPException
import aio_pika
from pydantic import BaseModel
import logging
from tenacity import retry, wait_fixed, stop_after_attempt
import uuid
import asyncio
import json
# OpenTelemetry
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

app = FastAPI()
FastAPIInstrumentor.instrument_app(app)

RABBITMQ_URL = "amqp://admin:admin@localhost:5672"
# Настроим провайдер трейсинга
trace.set_tracer_provider(TracerProvider())
from opentelemetry.sdk.resources import Resource
resource = Resource(attributes={"service.name": "Gateway-trace-app"})
trace.set_tracer_provider(TracerProvider(resource=resource))


# Экспорт в Jaeger
jaeger_exporter = JaegerExporter(
    agent_host_name="localhost",
    agent_port=6831,
)
span_processor = BatchSpanProcessor(jaeger_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# @app.get("/")
# def read_root():
#     return {"Hello": "World"}

# Модель для запроса через body
class MessageRequest(BaseModel):
    message: str

# Событие запуска приложения
@app.on_event("startup")
async def startup():
    try:
        # Попытка подключиться к RabbitMQ
        logger.info("Attempting to connect to RabbitMQ...")
        app.state.connection = await aio_pika.connect_robust(RABBITMQ_URL)
        app.state.channel = await app.state.connection.channel()
        app.state.exchange = await app.state.channel.declare_exchange(
            "messages", aio_pika.ExchangeType.DIRECT
        )
        app.state.callback_queue = await app.state.channel.declare_queue(exclusive=True)
        async def on_response(message: aio_pika.IncomingMessage):
            correlation_id = message.correlation_id
            if correlation_id in app.state.futures:
                app.state.futures[correlation_id].set_result(message.body)
      
        await app.state.callback_queue.consume(on_response)


        app.state.futures = {}
        logger.info("Successfully connected to RabbitMQ and declared exchange 'messages'")
    except Exception as e:
        # Логирование ошибки и завершение приложения
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        raise RuntimeError("Application failed to start due to RabbitMQ connection error")


# Событие остановки приложения
@app.on_event("shutdown")
async def shutdown():
    if hasattr(app.state, 'connection') and app.state.connection:
        await app.state.connection.close()
        logger.info("Disconnected from RabbitMQ")

# Конечная точка для отправки сообщений
@app.post("/send/")
async def send_message(request: MessageRequest):
    message = request.message
    correlation_id = str(uuid.uuid4())
    trace_id = str(uuid.uuid4())  # Создаём trace_id для запроса
    payload = {
            "trace_id": trace_id,
            "message": message
        }

    future = asyncio.get_event_loop().create_future()
    app.state.futures[correlation_id] = future

    logger.info(f"Received message: {message}")


    try:
        with tracer.start_as_current_span("gateway_send_message") as span:
                span.set_attribute("custom.trace_id", trace_id)
                with tracer.start_as_current_span("gateway_send_message") as span:
                        span.set_attribute("custom.trace_id", trace_id)
                        await app.state.exchange.publish(
                            aio_pika.Message(
                                body=json.dumps(payload).encode(),
                                reply_to=app.state.callback_queue.name,
                                correlation_id=correlation_id
                            ),
                            routing_key="service_queue"
                        )


        response = await future
        decoded_response = json.loads(response)
            # Логируем trace_id вместе с ответом
            # print(f"[Gateway][Trace ID: {trace_id}] Response: {decoded_response}")
        logger.info(f"[Gateway][Trace ID: {trace_id}] Response: {decoded_response}")
        return decoded_response
    except Exception as e:
        logger.error(f"Failed to publish message: {e}")
        raise HTTPException(status_code=500, detail="Failed to send message")

# Событие запуска приложения
@app.on_event("startup")
async def startup():
    try:
        # Попытка подключиться к RabbitMQ
        logger.info("Attempting to connect to RabbitMQ...")
        app.state.connection = await connect_to_rabbitmq()
        app.state.channel = await app.state.connection.channel()
        app.state.exchange = await app.state.channel.declare_exchange(
            "messages", aio_pika.ExchangeType.DIRECT
        )
        logger.info("Successfully connected to RabbitMQ and declared exchange 'messages'")
    except Exception as e:
        # Логирование ошибки и завершение приложения
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        raise RuntimeError("Application failed to start due to RabbitMQ connection error")

# Функция для подключения к RabbitMQ с повторными попытками
@retry(wait=wait_fixed(2), stop=stop_after_attempt(5))
async def connect_to_rabbitmq():
    logger.info("Attempting to connect to RabbitMQ...")
    return await aio_pika.connect_robust(RABBITMQ_URL)
