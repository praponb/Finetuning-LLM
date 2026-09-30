from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w_hist = Window.partitionBy("method").orderBy("paid_at", "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("is_failed").desc(), "id")
w4 = Window.partitionBy("method", "status").orderBy(F.col("order_id_stddev_samp_is_failed").desc_nulls_last())
w5 = Window.orderBy("is_failed", "id")
w6 = Window.partitionBy("method").orderBy("order_id_stddev_samp_is_failed", "id").rowsBetween(-3, -1)
w7 = Window.partitionBy("method").orderBy(F.col("is_failed_bucket").desc_nulls_first())
w8 = Window.partitionBy("status").orderBy(F.col("is_failed_bucket").desc_nulls_first())

filtered = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-09-16 22:00:00").cast("timestamp")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.when(F.col("status") == "FAILED", 1).otherwise(0).alias("is_failed"),
    )
)

enriched = (
    filtered
    .filter(F.col("is_failed") != 50)
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        "is_failed",
        F.stddev_samp("is_failed").over(w).alias("order_id_stddev_samp_is_failed"),
        F.lead("is_failed", 1, 0).over(w_hist).alias("next_is_failed"),
        F.ntile(3).over(w3).alias("is_failed_bucket"),
    )
)

enriched_payments = (
    enriched
    .select(
        "id",
        "paid_at",
        "status",
        "is_failed",
        "order_id_stddev_samp_is_failed",
        "is_failed_bucket",
        F.dense_rank().over(w4).alias("order_id_stddev_samp_is_failed_dense_rank"),
        F.lead("is_failed_bucket", 1).over(w5).alias("next_is_failed_bucket"),
        F.avg("is_failed").over(w6).alias("is_failed_avg_3_prior"),
        F.dense_rank().over(w7).alias("is_failed_bucket_dense_rank"),
    )
)

ranked = (
    enriched_payments
    .withColumn("latest_rank", F.rank().over(w8))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "order_id_stddev_samp_is_failed",
        "is_failed_bucket",
        "order_id_stddev_samp_is_failed_dense_rank",
        "next_is_failed_bucket",
        "is_failed_avg_3_prior",
        "is_failed_bucket_dense_rank",
    )
)

result = (
    ranked
    .select(
        "id",
        "order_id_stddev_samp_is_failed_dense_rank",
        "next_is_failed_bucket",
        "is_failed_avg_3_prior",
    )
)
