from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("country").orderBy("started_at", "id")
w_recent = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter(F.col("started_at") > F.to_date(F.lit("2023-02-16")))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.lower("device").alias("device_lower"),
    )
)

result = (
    cleaned
    .filter(F.hour("started_at").between(10, 15))
    .select(
        "id",
        "customer_id",
        "browser",
        F.row_number().over(w_hist).alias("event_seq"),
        F.last("browser", ignorenulls=True).over(w_recent).alias("last_known_browser"),
        F.last("country", ignorenulls=True).over(w_ordered).alias("last_known_country"),
    )
)
