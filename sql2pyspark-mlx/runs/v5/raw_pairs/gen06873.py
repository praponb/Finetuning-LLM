from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("latency_ms").desc_nulls_first())
w2 = Window.orderBy("latency_ms", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("latency_ms").asc_nulls_last())
w4 = Window.orderBy(F.col("status_code_high").asc_nulls_first())
w5 = Window.orderBy("status_code_high", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "latency_ms",
        "payload_json",
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.col("context").getItem("region").alias("region"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "latency_ms",
        "status_code_high",
        F.rank().over(w).alias("latency_ms_rank"),
        F.first("region", ignorenulls=True).over(w2).alias("first_known_region"),
        F.cume_dist().over(w3).alias("latency_ms_cume_dist"),
        F.cume_dist().over(w4).alias("status_code_high_cume_dist"),
        F.first("latency_ms", ignorenulls=True).over(w5).alias("first_known_latency_ms"),
    )
)
