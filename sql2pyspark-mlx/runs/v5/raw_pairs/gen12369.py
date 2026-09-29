from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount_p50").desc())

base = (
    df_payments
    .filter(F.col("method") == "card")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.date_trunc("WEEK", "paid_at").alias("paid_at_period"),
        F.coalesce("method", F.lit("OTHER")).alias("method_filled"),
    )
)

result = (
    base
    .groupBy("order_id", "method")
    .agg(
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.min("paid_at").alias("first_seen"),
        F.percentile_approx("amount", 0.5).alias("amount_p50_2"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
    )
    .withColumn("amount_p50_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("amount_p50").asc_nulls_last())
)
