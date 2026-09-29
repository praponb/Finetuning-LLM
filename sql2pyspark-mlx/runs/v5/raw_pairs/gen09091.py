from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount")
w_part = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("method")
w_recent = Window.orderBy("amount", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("method").orderBy("amount_avg_6", "id").rowsBetween(-2, -1)
w6 = Window.orderBy("seq_no", "id")
w7 = Window.partitionBy("method").orderBy("amount_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("method").orderBy("amount_rank", "id")
w9 = Window.orderBy(F.col("seq_no_avg_2_prior").asc_nulls_first())
w10 = Window.partitionBy("method").orderBy(F.col("method_sum_amount_share").desc(), "id")
w_running = Window.partitionBy("method").orderBy(F.col("amount_share").desc(), "id")
w12 = Window.partitionBy("seq_no_bucket_tier").orderBy(F.col("num_methods").desc())

cleaned = (
    df_payments
    .filter(F.col("order_id").isNotNull() & F.hour("paid_at").between(10, 17))
    .select("id", "method", "amount", F.lower("method").alias("method_lower"))
)

scored = (
    cleaned
    .select(
        "*",
        F.rank().over(w).alias("amount_rank"),
        F.row_number().over(w_part).alias("seq_no"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
        F.avg("amount").over(w_recent).alias("amount_avg_6"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share_2"),
    )
)

cleaned_payments = (
    scored
    .filter(F.col("method").isNotNull())
    .withColumn("amount_rank_str", F.col("amount_rank").cast("double"))
    .withColumn("resolved_method", F.coalesce("method", F.lit("UNKNOWN")))
)

scored_payments = (
    cleaned_payments
    .select(
        "*",
        F.avg("seq_no").over(w5).alias("seq_no_avg_2_prior"),
        F.ntile(4).over(w6).alias("seq_no_bucket"),
        F.last("amount_rank_str", ignorenulls=True).over(w7).alias("last_known_amount_rank_str"),
        F.sum("amount_share").over(w3).alias("method_sum_amount_share"),
        F.ntile(10).over(w8).alias("amount_rank_bucket"),
    )
)

enriched = (
    scored_payments
    .withColumn("amount_rank_bucket_share", F.round((F.col("amount_rank_bucket") / F.sum("amount_rank_bucket").over(w3)), 4))
    .withColumn("seq_no_avg_2_prior_dense_rank", F.dense_rank().over(w9))
    .withColumn("method_sum_amount_share_bucket", F.ntile(5).over(w10))
    .withColumn("seq_no_2", F.row_number().over(w_running))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("seq_no_bucket") >= 8, "HIGH").otherwise("LOW").alias("seq_no_bucket_tier"),
        F.when(F.col("seq_no_avg_2_prior") >= 8, "HIGH").otherwise("LOW").alias("seq_no_avg_2_prior_tier"),
        F.when(F.col("amount_rank") >= 6, "LARGE").otherwise("SMALL").alias("amount_rank_tier"),
    )
    .agg(
        F.count_if(F.col("method") == "cash").alias("cash_events"),
        F.sum(F.when(F.col("method") == "bank_transfer", F.col("seq_no"))).alias("bank_transfer_total_seq_no"),
        F.size(F.collect_set("method")).alias("num_methods"),
        F.round(F.sum("amount_rank_bucket_share") / F.nullif(F.count(F.when(F.col("method") == "cash", 1)), F.lit(0)), 2).alias("amount_rank_bucket_share_per_cash"),
    )
    .withColumn("num_methods_percent_rank", F.percent_rank().over(w12))
    .orderBy(F.col("seq_no_bucket_tier").asc_nulls_last())
)
