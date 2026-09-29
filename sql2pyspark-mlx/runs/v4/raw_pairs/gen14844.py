from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("is_success", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("method", "status").orderBy("amount", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("method").orderBy(F.col("is_success").desc(), "id")
w_ordered = Window.partitionBy("status", "method").orderBy(F.col("is_success").desc())
w_running = Window.orderBy("is_success_avg_6_prior", "id")
w_grp = Window.partitionBy("status").orderBy(F.col("is_success_avg_6_prior").desc(), "id")
w7 = Window.partitionBy("status").orderBy("is_success_avg_6_prior", "id").rowsBetween(-2, Window.currentRow)

base = (
    df_payments
    .filter((F.col("paid_at") > F.to_date(F.lit("2024-04-12"))) & F.col("order_id").isNotNull() & (F.to_date("paid_at") >= "2024-07-27"))
    .select(
        "id",
        "method",
        "amount",
        "status",
        F.when(F.col("status") == "SUCCESS", 1).otherwise(0).alias("is_success"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_clean"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.avg("is_success").over(w).alias("is_success_avg_6_prior"),
        F.avg("is_success").over(w2).alias("is_success_avg_6_prior_2"),
    )
)

valid = (
    with_windows
    .filter(F.col("method").isNotNull())
    .select("*", F.concat_ws(",", "method", "status").alias("method_status_key"))
)

enriched = (
    valid
    .withColumn("is_success_bucket", F.ntile(4).over(w3))
    .withColumn("is_success_cume_dist", F.cume_dist().over(w_ordered))
    .withColumn("row_seq", F.row_number().over(w_running))
    .withColumn("is_success_avg_6_prior_bucket", F.ntile(5).over(w_grp))
    .withColumn("is_success_avg_2", F.avg("is_success").over(w7))
)

valid_payments = (
    enriched
    .filter(F.col("method").isNotNull() & F.col("method").isNull())
    .withColumn("is_success_band", F.when(F.col("is_success") >= 60, "High").when(F.col("is_success") >= 31, "Medium").otherwise("Low"))
    .withColumn("row_seq_plus_is_success_avg_6_prior_2", (F.col("row_seq") + F.col("is_success_avg_6_prior_2")))
    .withColumn("method_filled", F.coalesce("method", F.lit("NONE")))
)

result = (
    valid_payments
    .groupBy(
        "method",
        "status",
        F.when(F.col("is_success_avg_2") >= 82, "HIGH").otherwise("LOW").alias("is_success_avg_2_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_rows"),
        F.round(F.avg("is_success_avg_6_prior"), 2).alias("avg_is_success_avg_6_prior"),
        F.count_if(F.col("status") == "SUCCESS").alias("success_events"),
    )
    .orderBy("method")
)
