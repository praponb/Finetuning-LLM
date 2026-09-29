from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-05-13").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.round((F.col("amount") * 1.15), 2).alias("amount_scaled"),
        F.lower("method").alias("method_lower"),
    )
)

with_gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("island_id") >= 8, "PREMIUM").otherwise("STANDARD").alias("island_id_tier"),
    )
    .agg(
        F.min("paid_at").alias("started_at"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("island_id_tier").asc_nulls_first())
)
