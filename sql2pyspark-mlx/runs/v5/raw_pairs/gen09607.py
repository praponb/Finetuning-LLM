from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_first())
w4 = Window.partitionBy("status").orderBy("amount", "id")
w5 = Window.partitionBy("status").orderBy("new_group_flag")

cleaned = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        F.when(F.col("amount") >= 1490, "High").when(F.col("amount") >= 730, "Medium").otherwise("Low").alias("amount_band"),
    )
)

flagged = (
    cleaned
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("new_group_flag").over(w2))

metrics = (
    islands
    .select(
        "*",
        F.rank().over(w3).alias("amount_rank"),
        F.ntile(10).over(w4).alias("amount_bucket"),
        F.percent_rank().over(w5).alias("new_group_flag_percent_rank"),
    )
)

cleaned_payments = (
    metrics
    .filter(F.col("paid_at") >= F.lit("2023-09-22 15:00:00").cast("timestamp"))
    .select(
        "*",
        F.when(F.col("amount_rank") >= 7, "High").when(F.col("amount_rank") >= 4, "Medium").otherwise("Low").alias("amount_rank_band"),
    )
)

result = (
    cleaned_payments
    .select(
        "id",
        "session_id",
        "amount_rank",
        "amount_bucket",
        "new_group_flag_percent_rank",
        "amount_rank_band",
    )
    .orderBy(F.col("id").asc_nulls_last())
)
