from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.partitionBy("key").orderBy(F.col("latency_ms").desc(), "id", "key")
w3 = Window.orderBy("latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("host", "key").orderBy(F.col("latency_ms").desc_nulls_first(), "id", "key")
w5 = Window.partitionBy("host", "key").orderBy(F.col("seq_no").desc_nulls_last(), "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

enriched = (
    flattened
    .select(
        "*",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share"),
        F.row_number().over(w2).alias("seq_no"),
        F.first("key", ignorenulls=True).over(w3).alias("first_known_key"),
    )
)

deduped = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

ranked = (
    deduped
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .select("id", "latency_ms", "key", "attr_value", "seq_no", "first_known_key", "rn")
    .orderBy(F.col("id").desc(), "seq_no")
)
