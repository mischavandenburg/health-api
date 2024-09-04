import os
import logging
import json
from typing import Any, Dict, List
from datetime import datetime, date

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, RootModel

import psycopg
from psycopg.sql import SQL, Identifier, Placeholder

app = FastAPI()

pg_host = os.getenv("DB_HOST", "localhost")
pg_database = os.getenv("DB_NAME", "postgres")
pg_user = os.getenv("DB_USER", "postgres")
pg_password = os.getenv("DB_PASSWORD", "postgres")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Define metric types for each category
DIET_METRICS = ["dietary_energy"]
BODY_COMPOSITION_METRICS = [
    "lean_body_mass",
    "body_mass_index",
    "weight_body_mass",
    "body_fat_percentage",
]


def create_pg_connection():
    """Create and return a PostgreSQL connection."""
    logger.info("Creating PostgreSQL connection")
    return psycopg.connect(
        f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
    )


def create_tables():
    """Create necessary tables if they don't exist."""
    logger.info("Creating tables if they don't exist")
    with create_pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS diet (
                    date DATE PRIMARY KEY,
                    dietary_energy FLOAT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS body_composition (
                    date DATE PRIMARY KEY,
                    lean_body_mass FLOAT,
                    body_mass_index FLOAT,
                    weight_body_mass FLOAT,
                    body_fat_percentage FLOAT
                )
            """)
        conn.commit()
    logger.info("Tables created successfully")


create_tables()


class MetricData(BaseModel):
    date: str
    qty: float


class Metric(BaseModel):
    name: str
    data: List[MetricData]


class HealthData(BaseModel):
    metrics: List[Metric]


class HealthDataWrapper(BaseModel):
    data: HealthData


def process_metrics(metrics: List[Metric], allowed_metrics: List[str], table_name: str):
    """
    Process metrics and insert/update them in the database.

    :param metrics: List of metrics to process
    :param allowed_metrics: List of metric names allowed for this table
    :param table_name: Name of the table to insert/update data
    :return: Number of processed metrics
    """
    processed_count = 0
    metrics_data: Dict[date, Dict[str, float]] = {}

    try:
        for metric in metrics:
            if metric.name not in allowed_metrics:
                logger.warning(
                    f"Skipping unknown metric type for {table_name}: {metric.name}"
                )
                continue

            for data_point in metric.data:
                metric_date = datetime.strptime(
                    data_point.date, "%Y-%m-%d %H:%M:%S %z"
                ).date()
                if metric_date not in metrics_data:
                    metrics_data[metric_date] = {}

                value = data_point.qty
                if metric.name == "dietary_energy":
                    value /= 4.184  # Convert kJ to kcal

                metrics_data[metric_date][metric.name] = value
                processed_count += 1

        logger.info(f"Processed {processed_count} metrics for {table_name}")
        logger.debug(f"Metrics data: {json.dumps(metrics_data, default=str)}")

        with create_pg_connection() as conn:
            with conn.cursor() as cur:
                for metric_date, date_metrics in metrics_data.items():
                    columns = list(date_metrics.keys())
                    placeholders = [Placeholder()] * len(columns)
                    values = [metric_date] + list(date_metrics.values())

                    query = SQL("""
                    INSERT INTO {table} (date, {columns})
                    VALUES (%s, {placeholders})
                    ON CONFLICT (date) DO UPDATE
                    SET {updates}
                    """).format(
                        table=Identifier(table_name),
                        columns=SQL(", ").join(map(Identifier, columns)),
                        placeholders=SQL(", ").join(placeholders),
                        updates=SQL(", ").join(
                            SQL("{} = EXCLUDED.{}").format(
                                Identifier(col), Identifier(col)
                            )
                            for col in columns
                        ),
                    )

                    try:
                        cur.execute(query, values)
                        logger.info(
                            f"Successfully inserted/updated data for date {metric_date} in {table_name}"
                        )
                    except psycopg.Error as e:
                        logger.error(
                            f"Database error while inserting/updating data for date {metric_date} in {table_name}: {str(e)}"
                        )
                        logger.error(f"Query: {query.as_string(conn)}")
                        logger.error(f"Values: {values}")
                        raise

            conn.commit()
            logger.info(f"Successfully committed all changes for {table_name}")

    except Exception as e:
        logger.exception(
            f"Unexpected error while processing metrics for {table_name}: {str(e)}"
        )
        raise

    return processed_count


@app.post("/dietary-energy/")
async def add_dietary_energy(data: HealthDataWrapper):
    """
    Add dietary energy data to the database.

    :param data: HealthDataWrapper containing dietary energy data
    :return: JSON response indicating success or failure
    """
    try:
        logger.info("Received request to add dietary energy data")
        processed_count = process_metrics(data.data.metrics, DIET_METRICS, "diet")
        logger.info(
            f"Successfully processed {processed_count} dietary energy data points"
        )
        return {
            "message": f"Processed {processed_count} dietary energy data points successfully"
        }
    except Exception as e:
        logger.exception(f"Error processing dietary energy data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.post("/body-composition/")
async def add_body_composition(data: HealthDataWrapper):
    """
    Add body composition data to the database.

    :param data: HealthDataWrapper containing body composition data
    :return: JSON response indicating success or failure
    """
    try:
        logger.info("Received request to add body composition data")
        processed_count = process_metrics(
            data.data.metrics, BODY_COMPOSITION_METRICS, "body_composition"
        )
        logger.info(
            f"Successfully processed {processed_count} body composition data points"
        )
        return {
            "message": f"Processed {processed_count} body composition data points successfully"
        }
    except Exception as e:
        logger.exception(f"Error processing body composition data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


class AnyJSON(RootModel):
    root: Any


@app.post("/echo/")
async def echo(data: AnyJSON):
    """
    Echo the received JSON data.

    :param data: Any JSON data
    :return: The same JSON data, echoed back
    """
    logger.info("Received echo request")
    logger.debug(f"Echo data: {data.root}")
    return JSONResponse(content=data.root)


if __name__ == "__main__":
    import uvicorn

    logger.info("Starting the application")
    uvicorn.run(app, host="0.0.0.0", port=8000)

