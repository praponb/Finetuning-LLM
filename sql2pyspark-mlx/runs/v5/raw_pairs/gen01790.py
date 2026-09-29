from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("method")
w4 = Window.partitionBy("method").orderBy("last4_min_amount", "id").rowsBetween(-6, -1)
w_running = Window.orderBy(F.col("last4_avg_amount").desc(), "id")
w6 = Window.partitionBy("method").orderBy("seq_no", "id")
w7 = Window.orderBy("amount_max_30d", "id")
w8 = Window.partitionBy("method").orderBy(F.col("seq_no_2").desc(), "id")

base = (
    df_payments
    .filter(F.col("paid_at") >= F.to_date(F.lit("2023-09-14")))
    .select(
        "*",
        F.concat("status", F.lit(" - "), "method").alias("status_method"),
        F.when(F.col("amount") >= 1740, "High").when(F.col("amount") >= 260, "Medium").otherwise("Low").alias("amount_band"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("resolved_method"),
    )
)

with_windows = (
    base
    .filter(F.col("order_id").isNull())
    .select(
        "id",
        "method",
        "paid_at",
        "amount_band",
        F.count("amount").over(w_ordered).alias("last4_count_amount"),
        F.min("amount").over(w_ordered).alias("last4_min_amount"),
        F.avg("amount").over(w_ordered).alias("last4_avg_amount"),
        F.max("amount").over(w2).alias("amount_max_30d"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "method",
        "amount_band",
        "last4_min_amount",
        "last4_avg_amount",
        "amount_max_30d",
        F.max("last4_count_amount").over(w3).alias("method_max_last4_count_amount"),
        F.avg("last4_avg_amount").over(w4).alias("last4_avg_amount_avg_6_prior"),
        F.row_number().over(w_running).alias("seq_no"),
    )
)

enriched = (
    metrics
    .withColumn("prev_last4_avg_amount", F.lag("last4_avg_amount").over(w6))
    .withColumn("prev_amount_max_30d", F.lag("amount_max_30d").over(w6))
    .withColumn("next_amount_max_30d", F.lead("amount_max_30d").over(w6))
    .withColumn("seq_no_2", F.row_number().over(w6))
    .withColumn("amount_max_30d_bucket", F.ntile(4).over(w7))
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w8))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("last4_min_amount") >= 1290, "LARGE").otherwise("SMALL").alias("last4_min_amount_tier"),
    )
    .agg(
        F.percentile_approx("last4_avg_amount", [0.25, 0.5, 0.75]).alias("last4_avg_amount_quartiles"),
        F.percentile_approx("amount_max_30d_bucket", [0.25, 0.5, 0.75]).alias("amount_max_30d_bucket_quartiles"),
        F.round(F.sum("amount_max_30d_bucket") / F.nullif(F.count(F.when(F.col("method") == "bank_transfer", 1)), F.lit(0)), 2).alias("amount_max_30d_bucket_per_bank_transfer"),
    )
    .filter(F.col("amount_max_30d_bucket_per_bank_transfer") > 0)
    .orderBy(F.col("amount_max_30d_bucket_per_bank_transfer").desc_nulls_first())
)
