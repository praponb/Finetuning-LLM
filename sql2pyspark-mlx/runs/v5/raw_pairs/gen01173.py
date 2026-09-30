from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("customer_id").orderBy("amount", "id")
w2 = Window.partitionBy("channel").orderBy(F.col("discount").desc(), "id")
w3 = Window.partitionBy("customer_id")
w4 = Window.partitionBy("store_id").orderBy("amount", "id")

metrics = (
    df_orders
    .filter(F.col("order_date").between("2024-02-28", "2025-02-01"))
    .withColumn("amount_bucket", F.ntile(4).over(w_hist))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w3)), 4))
    .withColumn("seq_no_2", F.row_number().over(w4))
)

result = (
    metrics
    .groupBy(
        "customer_id",
        F.when(F.col("amount_bucket") >= 7, "LARGE").otherwise("SMALL").alias("amount_bucket_tier"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("shipping_fee"))).alias("cancelled_total_shipping_fee"),
        F.sum("amount").alias("total_amount"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.percentile_approx("shipping_fee", [0.25, 0.5, 0.75]).alias("shipping_fee_quartiles"),
        F.countDistinct("channel").alias("distinct_channels"),
    )
    .filter(F.col("distinct_store_ids") > 1)
    .orderBy(F.col("customer_id").desc())
)
