import asyncio
import aio_pika
from tenacity import retry, wait_fixed, stop_after_attempt
import logging
import json

# OpenTelemetry
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace.export import BatchSpanProcessor

import time
# Prometheus
from prometheus_client import start_http_server, Counter, Histogram

# Настроим провайдер трейсинга
trace.set_tracer_provider(TracerProvider())

from opentelemetry.sdk.resources import Resource
resource = Resource(attributes={"service.name": "Gateway-trace-app"})
trace.set_tracer_provider(TracerProvider(resource=resource))


# Настройка
otlp_exporter = OTLPSpanExporter(endpoint="localhost:4317", insecure=True)


span_processor = BatchSpanProcessor(otlp_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)


RABBITMQ_URL = "amqp://admin:admin@localhost:5672"

MESSAGES_PROCESSED = Counter("messages_processed_total", "Total number of messages processed")
MESSAGES_ERRORS = Counter("messages_processing_errors_total", "Total number of processing errors")
MESSAGE_PROCESSING_TIME = Histogram("message_processing_duration_seconds", "Time spent processing message")

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@retry(wait=wait_fixed(2), stop=stop_after_attempt(5))
async def connect_to_rabbitmq():
    logger.info("Attempting to connect to RabbitMQ...")
    return await aio_pika.connect_robust(RABBITMQ_URL)

async def main():
    # Запуск сервера метрик Prometheus
    start_http_server(8001)
    logger.info("Prometheus metrics server started on http://localhost:8001")    
    connection = None
    try:
        # Подключение к RabbitMQ
        connection = await connect_to_rabbitmq()
        channel = await connection.channel()


        # Объявление обменника
        exchange = await channel.declare_exchange("messages", aio_pika.ExchangeType.DIRECT)


        # Объявление очереди
        queue = await channel.declare_queue("service_queue", durable=True)
        await queue.bind(exchange, routing_key="service_queue")


        logger.info(f"Received message: {message.body.decode()}")
        incoming = message.body.decode()
        print(f"Received: {incoming}")


        # Генерируем ответ
        response_text = f"Processed: {incoming.upper()}"


        if message.reply_to:
            await channel.default_exchange.publish(
            aio_pika.Message(
                body=response_text.encode(),
                correlation_id=message.correlation_id,
                span = trace.get_current_span()
            ),
        routing_key=message.reply_to
        )



        # Чтение сообщений
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    start_time = time.time()
                    try:
                        incoming_data = json.loads(message.body)
                        trace_id = incoming_data.get("trace_id")
                        incoming_message = incoming_data.get("message")


                        with tracer.start_as_current_span("service_process_message") as span:
                            span.set_attribute("custom.trace_id", trace_id)
                            print(f"[Service][Trace ID: {trace_id}] Received: {incoming_message}")


                            # Обработка сообщения
                            response_text = f"Processed: {incoming_message.upper()}"


                            # Отправка ответа, если задан reply_to
                            if message.reply_to:
                                response_payload = {
                                    "trace_id": trace_id,
                                    "result": response_text
                                }
                                await channel.default_exchange.publish(
                                    aio_pika.Message(
                                        body=json.dumps(response_payload).encode(),
                                        correlation_id=message.correlation_id
                                    ),
                                    routing_key=message.reply_to
                                )


                        MESSAGES_PROCESSED.inc()  # Увеличить счётчик успешной обработки


                    except Exception as e:
                        MESSAGES_ERRORS.inc()
                        logger.error(f"Error while processing message: {e}")


                    finally:
                        MESSAGE_PROCESSING_TIME.observe(time.time() - start_time)
                    start_time = time.time()
                    incoming_data = json.loads(message.body)
                    trace_id = incoming_data.get("trace_id")
                    incoming_message = incoming_data.get("message")
                    # Спан на обработку сообщения
                    with tracer.start_as_current_span("service_process_message") as span:
                        span.set_attribute("custom.trace_id", trace_id)
                        print(
                            f"[Service][Trace ID: {trace_id}] Received: {incoming_message}")


                        # Обработка
                        response_text = f"Processed: {incoming_message.upper()}"


                        if message.reply_to:
                            response_payload = {
                                "trace_id": trace_id,
                                "result": response_text
                            }
                            await channel.default_exchange.publish(
                                aio_pika.Message(
                                    body=json.dumps(response_payload).encode(),
                                    correlation_id=message.correlation_id,
                                    span = trace.get_current_span()
                                ),
                                routing_key=message.reply_to
                            )




    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise
    finally:
        if connection:
            await connection.close()
            logger.info("Disconnected from RabbitMQ")


if __name__ == "__main__":
    asyncio.run(main())


