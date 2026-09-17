"""K-means clustering on a sample of taxi trips, grouping them into
behavioral segments (e.g. short/cheap, long/expensive) by trip_distance,
fare_amount, and trip_duration_minutes.

Runs entirely outside Databricks/dbt: pulls a sample from fact_trips over
the Databricks SQL connector, clusters it locally with scikit-learn, and
writes the labeled sample back to a new trip_clusters table so Power BI can
read it like any other DirectQuery table. This sidesteps Power BI's
"Automatically find clusters" feature, which isn't available for
DirectQuery-connected visuals.

Requires DATABRICKS_HOST / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN in .env
(see .env.example) -- same credentials as ~/.dbt/profiles.yml's premium
target.
"""

import os

from databricks import sql
from dotenv import load_dotenv
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

load_dotenv()

CATALOG_SCHEMA = "nyc_taxi.nyc_taxi_lakehouse"
SAMPLE_SIZE = 5000
N_CLUSTERS = 4
FEATURES = ["trip_distance", "fare_amount", "trip_duration_minutes"]


def _connect():
    return sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


def fetch_sample(connection):
    query = f"""
        select trip_id, trip_distance, fare_amount, trip_duration_minutes
        from {CATALOG_SCHEMA}.fact_trips
        tablesample ({SAMPLE_SIZE} rows)
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        columns = [c[0] for c in cursor.description]
        rows = cursor.fetchall()
    return columns, rows


def cluster(columns, rows):
    feature_idx = [columns.index(f) for f in FEATURES]
    X = [[row[i] for i in feature_idx] for row in rows]

    X_scaled = StandardScaler().fit_transform(X)
    labels = KMeans(n_clusters=N_CLUSTERS, random_state=42, n_init=10).fit_predict(
        X_scaled
    )

    trip_id_idx = columns.index("trip_id")
    return [
        (row[trip_id_idx], *[row[i] for i in feature_idx], int(label))
        for row, label in zip(rows, labels)
    ]


def _sql_literal(value):
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)


def write_results(connection, labeled_rows):
    with connection.cursor() as cursor:
        cursor.execute(f"""
            create table if not exists {CATALOG_SCHEMA}.trip_clusters (
                trip_id STRING,
                trip_distance DOUBLE,
                fare_amount DOUBLE,
                trip_duration_minutes DOUBLE,
                cluster INT
            ) USING DELTA
        """)
        cursor.execute(f"delete from {CATALOG_SCHEMA}.trip_clusters")

        # executemany's %s placeholders aren't substituted by this connector
        # version -- build one multi-row INSERT with literal values instead.
        # Safe here since every value originates from our own warehouse
        # query, not untrusted input.
        value_rows = ", ".join(
            "(" + ", ".join(_sql_literal(v) for v in row) + ")"
            for row in labeled_rows
        )
        cursor.execute(f"insert into {CATALOG_SCHEMA}.trip_clusters values {value_rows}")


def main():
    connection = _connect()
    try:
        columns, rows = fetch_sample(connection)
        print(f"Pulled {len(rows)} sample trips")

        labeled_rows = cluster(columns, rows)
        print(f"Clustered into {N_CLUSTERS} groups")

        write_results(connection, labeled_rows)
        print(f"Wrote results to {CATALOG_SCHEMA}.trip_clusters")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
