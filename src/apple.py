import os
import logging
from typing import Any
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import RootModel

import psycopg

app = FastAPI()

pg_host = os.getenv("DB_HOST", "localhost")
pg_database = os.getenv("DB_NAME", "postgres")
pg_user = os.getenv("DB_USER", "postgres")
pg_password = os.getenv("DB_PASSWORD", "postgres")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def create_pg_connection():
    """Create and return a PostgreSQL connection."""
    logger.info("Creating PostgreSQL connection")
    return psycopg.connect(
        f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
    )


def create_tables():
    """Create necessary tables if they don't exist."""
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
                    weight_body_mass FLOAT
                )
            """)
        conn.commit()


create_tables()


@app.post("/dietary-energy/")
async def add_dietary_energy(data: dict):
    """
    Add dietary energy data to the database.

    :param data: Dictionary containing dietary energy data
    :return: JSON response indicating success or failure
    """
    try:
        metrics = data.get("data", {}).get("metrics", [])
        for metric in metrics:
            if metric["name"] == "dietary_energy":
                for data_point in metric["data"]:
                    date = datetime.strptime(
                        data_point["date"], "%Y-%m-%d %H:%M:%S %z"
                    ).date()
                    energy_kj = data_point["qty"]
                    energy_kcal = energy_kj / 4.184  # Convert kJ to kcal
                    with create_pg_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                INSERT INTO diet (date, dietary_energy)
                                VALUES (%s, %s)
                                ON CONFLICT (date) DO UPDATE
                                SET dietary_energy = EXCLUDED.dietary_energy
                                """,
                                (date, energy_kcal),
                            )
                        conn.commit()
        return {"message": "Dietary energy data processed successfully"}
    except Exception as e:
        logger.error(f"Error processing dietary energy data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/body-composition/")
async def add_body_composition(data: dict):
    """
    Add body composition data to the database.

    :param data: Dictionary containing body composition data
    :return: JSON response indicating success or failure
    """
    try:
        metrics = data.get("data", {}).get("metrics", [])
        body_comp_data = {}
        for metric in metrics:
            if metric["name"] in [
                "lean_body_mass",
                "body_mass_index",
                "weight_body_mass",
                "body_fat_percentage",
            ]:
                for data_point in metric["data"]:
                    date = datetime.strptime(
                        data_point["date"], "%Y-%m-%d %H:%M:%S %z"
                    ).date()
                    if date not in body_comp_data:
                        body_comp_data[date] = {}
                    body_comp_data[date][metric["name"]] = data_point["qty"]

        with create_pg_connection() as conn:
            with conn.cursor() as cur:
                for date, metrics in body_comp_data.items():
                    if len(metrics) != 3:
                        logger.warning(f"Incomplete data for date {date}: {metrics}")
                        continue

                    cur.execute(
                        """
                        INSERT INTO body_composition 
                        (date, lean_body_mass, body_mass_index, weight_body_mass, body_fat_percentage)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (date) DO UPDATE
                        SET lean_body_mass = EXCLUDED.lean_body_mass,
                            body_mass_index = EXCLUDED.body_mass_index,
                            weight_body_mass = EXCLUDED.weight_body_mass
                            body_fat_percentage = EXCLUDED.body_fat_percentage
                        """,
                        (
                            date,
                            metrics["lean_body_mass"],
                            metrics["body_mass_index"],
                            metrics["weight_body_mass"],
                            metrics["body_fat_percentage"],
                        ),
                    )
            conn.commit()
        return {"message": "Body composition data processed successfully"}
    except Exception as e:
        logger.error(f"Error processing body composition data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


class AnyJSON(RootModel):
    root: Any


@app.post("/echo/")
async def echo(data: AnyJSON):
    """
    Echo the received JSON data.

    :param data: Any JSON data
    :return: The same JSON data, echoed back
    """
    logger.info(data.root)
    return JSONResponse(content=data.root)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
