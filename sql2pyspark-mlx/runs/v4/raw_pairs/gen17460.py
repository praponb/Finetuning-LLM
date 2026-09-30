from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("order_id", "method").orderBy("amount_int", "id")
w_part = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(-2, Window.currentRow)
w_recent = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("amount", "id")
w5 = Window.partitionBy("amount_avg_90d_tier").orderBy(F.col("distinct_order_ids").desc())

valid = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2024-05-27").cast("date"))
    .select("*", F.col("amount").cast("int").alias("amount_int"))
)

scored = (
    valid
    .select(
        "*",
        F.lag("amount_int", 1).over(w_ordered).alias("prev_amount_int"),
        F.avg("amount_int").over(w_part).alias("amount_int_avg_2"),
        F.avg("amount").over(w_recent).alias("amount_avg_90d"),
        F.lag("amount_int", 1).over(w4).alias("prev_amount_int_2"),
    )
)

base = (
    scored
    .filter((F.col("paid_at") >= F.to_timestamp(F.lit("2024-04-15 04:00:00"))) & (F.col("prev_amount_int") == 65))
    .withColumn("is_bank_transfer", F.when(F.col("method") == "bank_transfer", 1).otherwise(0))
)

result = (
    base
    .groupBy(
        F.when(F.col("amount_avg_90d") >= 1714, "HIGH").otherwise("LOW").alias("amount_avg_90d_tier"),
        F.date_trunc("WEEK", "paid_at").alias("week_start"),
    )
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.countDistinct("order_id").alias("distinct_order_ids_2"),
    )
    .withColumn("distinct_order_ids_dense_rank", F.dense_rank().over(w5))
)
