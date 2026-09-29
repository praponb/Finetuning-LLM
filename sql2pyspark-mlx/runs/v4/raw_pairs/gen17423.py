from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("status", "order_id")
w3 = Window.partitionBy("method", "status").orderBy("paid_at", "id").rowsBetween(-3, -1)
w4 = Window.orderBy(F.col("is_cash").desc(), "id")
w5 = Window.partitionBy("order_id")
w6 = Window.partitionBy("order_id").orderBy("is_cash", "id")
w7 = Window.partitionBy("status").orderBy("status_min_is_cash", "id").rowsBetween(-5, Window.currentRow)
w8 = Window.partitionBy("status").orderBy("paid_at", "id")
w9 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter(F.col("method").isNotNull())
    .withColumn("amount_band", F.when(F.col("amount") >= 1590, "High").when(F.col("amount") >= 530, "Medium").otherwise("Low"))
    .withColumn("is_cash", F.when(F.col("method") == "cash", 1).otherwise(0))
    .withColumn("resolved_method", F.coalesce("method", F.lit("N/A")))
)

scored = (
    valid
    .select(
        "*",
        F.min("is_cash").over(w).alias("status_min_is_cash"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
        F.avg("is_cash").over(w3).alias("is_cash_avg_3_prior"),
        F.ntile(10).over(w4).alias("is_cash_bucket"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.round((F.col("amount") / F.sum("amount").over(w5)), 4).alias("amount_share_2"),
        F.ntile(10).over(w6).alias("is_cash_bucket_2"),
        F.avg("amount_share").over(w7).alias("amount_share_avg_5"),
    )
)

gaps = (
    enriched
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w8).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w8) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("is_new_burst").over(w9))

result = (
    islands
    .groupBy(
        F.when(F.col("is_cash") >= 87, "LARGE").otherwise("SMALL").alias("is_cash_tier"),
        "order_id",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("status_min_is_cash", [0.25, 0.5, 0.75]).alias("status_min_is_cash_quartiles"),
        F.sum(F.when(F.col("method") == "bank_transfer", F.col("amount_share")).otherwise(0)).alias("bank_transfer_amount_share"),
        F.max("paid_at").alias("last_paid_at"),
    )
)
