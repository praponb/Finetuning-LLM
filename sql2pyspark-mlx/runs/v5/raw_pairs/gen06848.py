from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method")
w2 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("paid_at", "id")
w4 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy(F.col("is_new_burst").desc_nulls_last(), "id")

base = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-05-14 17:00:00").cast("timestamp")) & (F.col("paid_at") >= "2024-03-13"))
    .select(
        "*",
        F.when(F.col("amount") >= 1300, "High").when(F.col("amount") >= 240, "Medium").otherwise("Low").alias("amount_tier"),
        F.coalesce("method", F.lit("NONE")).alias("method_filled"),
    )
)

with_windows = (
    base
    .filter(F.col("order_id") == 12)
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "method_filled",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.last("amount", ignorenulls=True).over(w2).alias("last_known_amount"),
    )
)

with_gaps = (
    with_windows
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w3).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w3) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w4))

latest = (
    islands
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "order_id", "amount_share", "burst_id")
)

result = latest.select("id", "order_id", "amount_share", "burst_id")
