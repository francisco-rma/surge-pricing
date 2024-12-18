import datetime
import logging
import os
import time
from contextlib import contextmanager

import h3
import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = 6379

DRIVER_POSITION_STREAM = "driver_position_stream"
DRIVER_COUNT_KEY = "driver_count_by_region"

RESOLUTIONS = [7, 8, 9]
STREAM_READ_TIMEOUT = 2000
SLEEP_INTERVAL = 0.1
BATCH_SIZE = 10
CONSUMER_GROUP_NAME = "driver_position_consumer_group"
CONSUMER_NAME = "consumer_1"


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()


@contextmanager
def get_redis_client():
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    try:
        yield client
    finally:
        client.close()


def get_h3_cells(latitude, longitude, resolutions):
    return {res: h3.latlng_to_cell(latitude, longitude, res) for res in resolutions}


def update_driver_count(client, h3_cells, timestamp):
    time_key = timestamp[:16]
    with client.pipeline() as pipe:
        for res, h3_cell in h3_cells.items():
            resolution_key = f"{DRIVER_COUNT_KEY}:{time_key}:{res}"
            logger.info(f"Resolution KEY {resolution_key}")
            pipe.hincrby(resolution_key, h3_cell, 1)
        pipe.execute()


def create_consumer_group(client):
    """Create the consumer group if it doesn't exist."""
    try:
        client.xgroup_create(DRIVER_POSITION_STREAM, CONSUMER_GROUP_NAME, id="0")
        logger.info("Consumer group created successfully.")
    except redis.exceptions.ResponseError as e:
        # The consumer group may already exist, handle this case
        if "BUSYGROUP" in str(e):
            logger.info("Consumer group already exists.")
        else:
            logger.error(f"Unexpected error creating consumer group: {e}")


def consume_messages_and_aggregate(client):
    logger.info("Starting Driver Position Aggregator...")

    try:
        while True:
            # Read new messages from the stream
            response = client.xreadgroup(
                CONSUMER_GROUP_NAME,
                CONSUMER_NAME,
                {DRIVER_POSITION_STREAM: ">"},  # '>' means read only new messages
                count=BATCH_SIZE,
                block=STREAM_READ_TIMEOUT,
            )
            if response:
                stream_name, messages = response[0]
                logger.info(f"Processing {len(messages)} messages from {stream_name}")
                for message_id, driver_position in messages:
                    try:
                        latitude = float(driver_position["latitude"])
                        longitude = float(driver_position["longitude"])
                        timestamp = driver_position["timestamp"]
                        h3_cells = get_h3_cells(latitude, longitude, RESOLUTIONS)
                        update_driver_count(client, h3_cells, timestamp)
                        logger.debug(
                            f"Updated driver counts for {h3_cells} at {timestamp}"
                        )
                        # Acknowledge the message after processing
                        client.xack(
                            DRIVER_POSITION_STREAM, CONSUMER_GROUP_NAME, message_id
                        )
                    except Exception as e:
                        logger.error(f"Error processing message {message_id}: {e}")
            else:
                logger.info("No new messages, sleeping...")
                time.sleep(SLEEP_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Aggregator stopped by user.")
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Redis connection error: {e}")
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
    finally:
        logger.info("Driver Position Aggregator shutting down.")


def claim_unacknowledged_messages(
    client, min_idle_time=60000, new_consumer=CONSUMER_NAME
):
    """
    Reivindica mensagens pendentes para o consumidor atual.

    :param client: Conexão Redis.
    :param min_idle_time: Tempo mínimo (em milissegundos) desde que a mensagem foi vista pela última vez.
    :param new_consumer: Nome do consumidor que reivindicará as mensagens.
    """
    try:
        pending_info = client.xpending_range(
            DRIVER_POSITION_STREAM,
            CONSUMER_GROUP_NAME,
            min="-",
            max="+",
            count=10,
            consumer=None,
        )

        if pending_info:
            logger.info(f"Reivindicando {len(pending_info)} mensagens pendentes...")
            for msg in pending_info:
                message_id = msg["message_id"]

                reclaimed_messages = client.xclaim(
                    DRIVER_POSITION_STREAM,
                    CONSUMER_GROUP_NAME,
                    new_consumer,
                    min_idle_time,
                    message_id,
                )
                if reclaimed_messages:
                    logger.info(f"Mensagem {message_id} reivindicada com sucesso.")
                else:
                    logger.warning(f"Falha ao reivindicar a mensagem {message_id}.")
        else:
            logger.info("Nenhuma mensagem pendente para reivindicar.")
    except Exception as e:
        logger.error(f"Erro ao processar mensagens pendentes: {e}")


def main():
    with get_redis_client() as client:

        CLAIM_INTERVAL = 60
        last_claim_time = 0

        create_consumer_group(client)

        try:
            while True:

                current_time = time.time()
                if current_time - last_claim_time >= CLAIM_INTERVAL:
                    claim_unacknowledged_messages(client)
                    last_claim_time = current_time

                consume_messages_and_aggregate(client)
        except KeyboardInterrupt:
            logger.info("Aggregator stopped by user.")
        except redis.exceptions.ConnectionError as e:
            logger.error(f"Redis connection error: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error: {e}")
        finally:
            logger.info("Shutting down.")


if __name__ == "__main__":
    main()
