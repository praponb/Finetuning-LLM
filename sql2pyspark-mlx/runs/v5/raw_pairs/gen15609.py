from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id", "device").orderBy(F.col("started_at").desc(), "id")
w_part = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id", "device").orderBy("started_at", "id")
w5 = Window.orderBy("started_at", "id")

filtered = (
    df_sessions
    .filter((F.col("started_at") > F.to_timestamp(F.lit("2023-06-13 18:00:00"))) & (F.col("country") == "UK"))
    .withColumn("device_upper", F.upper("device"))
    .withColumn("browser_filled", F.coalesce("browser", F.lit("N/A")))
)

result = (
    filtered
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "device_upper",
        F.last("country", ignorenulls=True).over(w_running).alias("last_known_country"),
        F.row_number().over(w2).alias("row_seq"),
        F.first("country", ignorenulls=True).over(w_part).alias("first_known_country"),
        F.row_number().over(w4).alias("row_seq_2"),
        F.row_number().over(w5).alias("event_seq"),
    )
)
