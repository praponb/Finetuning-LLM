from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy("paid_at", "id").rowsBetween(-3, -1)
w2 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("amount_count_7d", "id")

valid = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2024-03-12 16:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .withColumn("clean_status", F.trim("status"))
    .withColumn("method_filled", F.coalesce("method", F.lit("DIRECT")))
)

scored = (
    valid
    .withColumn("amount_avg_3_prior", F.avg("amount").over(w))
    .withColumn("amount_count_7d", F.count("amount").over(w2))
)

deduped = (
    scored
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .groupBy("method", "order_id")
    .pivot("status", ["REFUNDED", "SUCCESS"])
    .agg(F.sum("amount").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "method",
        "order_id",
        F.col("REFUNDED_value_sum").alias("refunded_value_sum"),
        F.col("REFUNDED_item_count").alias("refunded_item_count"),
        F.col("SUCCESS_value_sum").alias("success_value_sum"),
        F.col("SUCCESS_item_count").alias("success_item_count"),
    )
)
