from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_start_flag_tier").orderBy(F.col("event_count").desc())

filtered = (
    df_payments
    .filter(F.col("paid_at") > F.to_date(F.lit("2023-11-01")))
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        F.when(F.col("status") == "SUCCESS", 1).otherwise(0).alias("is_success"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_clean"),
    )
)

gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("session_start_flag") >= 1, "HIGH").otherwise("LOW").alias("session_start_flag_tier"),
        F.date_trunc("YEAR", "paid_at").alias("year_start"),
    )
    .agg(
        F.percentile_approx("session_start_flag", [0.25, 0.5, 0.75]).alias("session_start_flag_quartiles"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("event_count") > 1)
    .withColumn("event_count_dense_rank", F.dense_rank().over(w3))
)
