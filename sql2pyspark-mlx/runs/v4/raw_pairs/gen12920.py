from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country", "browser").orderBy("started_at", "id")

base = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-11-10 00:00:00").cast("timestamp")) & F.col("customer_id").isNotNull() & (F.col("country") == "UK"))
    .withColumn("device_title", F.initcap(F.trim("device")))
)

result = (
    base
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.first("country", ignorenulls=True).over(w2).alias("first_known_country"),
        F.row_number().over(w3).alias("event_seq"),
    )
)
