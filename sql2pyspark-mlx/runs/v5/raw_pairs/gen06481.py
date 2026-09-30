from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method")
w4 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w5 = Window.partitionBy("method").orderBy(F.col("amount").desc_nulls_first())
w6 = Window.partitionBy("status").orderBy(F.col("amount_percent_rank_whole").desc_nulls_last())

valid = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "REFUNDED", "SUCCESS"]) & F.col("order_id").isNotNull())
    .withColumn("amount_rounded", F.round("amount", 1))
)

with_gaps = (
    valid
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

with_windows = (
    islands
    .filter(F.col("method") == "paypal")
    .select(
        "id",
        "method",
        "status",
        "burst_id",
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
        F.count("burst_id").over(w4).alias("burst_id_count_90d"),
        F.percent_rank().over(w5).alias("amount_percent_rank"),
    )
)

filtered = (
    with_windows
    .filter(~F.col("method").isin(["bank_transfer", "card"]))
    .select(
        "id",
        "status",
        "amount_share",
        "burst_id_count_90d",
        "amount_percent_rank",
        F.round((F.col("amount_percent_rank") * 1.1), 2).alias("amount_percent_rank_scaled"),
        F.ceil("amount_percent_rank").alias("amount_percent_rank_whole"),
    )
)

ranked = (
    filtered
    .withColumn("row_num", F.rank().over(w6))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .select(
        "id",
        "status",
        "amount_share",
        "amount_percent_rank",
        "amount_percent_rank_scaled",
        "row_num",
    )
)
