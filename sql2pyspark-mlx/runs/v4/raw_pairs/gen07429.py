from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("started_at", "id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("browser_length").asc_nulls_first())

cleaned = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-12-10").cast("date")) & F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.length("browser").alias("browser_length"),
    )
)

result = (
    cleaned
    .select(
        "*",
        F.sum("browser_length").over(w_ordered).alias("running_browser_length"),
        F.row_number().over(w2).alias("event_seq"),
        F.dense_rank().over(w3).alias("browser_length_dense_rank"),
    )
)
