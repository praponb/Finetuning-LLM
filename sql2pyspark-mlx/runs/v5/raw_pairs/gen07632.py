from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(-3, Window.currentRow)
w_ordered_2 = Window.orderBy(F.col("amount").desc_nulls_first())
w3 = Window.orderBy("amount", "id")
w4 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("status").orderBy(F.col("amount_scaled").asc_nulls_last())

cleaned = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-08-20 15:00:00").cast("timestamp")) & F.col("order_id").isNull())
    .withColumn("amount_scaled", F.round((F.col("amount") * 1.1), 2))
    .withColumn("paid_at_label", F.date_format("paid_at", "MMM yyyy"))
    .withColumn("resolved_method", F.coalesce("method", F.lit("OTHER")))
)

result = (
    cleaned
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        "amount_scaled",
        "paid_at_label",
        "resolved_method",
        F.avg("amount_scaled").over(w_ordered).alias("amount_scaled_avg_3"),
        F.percent_rank().over(w_ordered_2).alias("amount_percent_rank"),
        F.ntile(3).over(w3).alias("amount_bucket"),
        F.sum("amount").over(w4).alias("cumulative_amount"),
        F.cume_dist().over(w_recent).alias("amount_scaled_cume_dist"),
    )
)
