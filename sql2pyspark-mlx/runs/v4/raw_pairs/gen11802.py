from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc())
w2 = Window.partitionBy("customer_id").orderBy("latest_rank", "id")
w3 = Window.partitionBy("customer_id").orderBy("amount", "id").rowsBetween(-5, Window.currentRow)

base = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .withColumn("channel_status", F.concat("channel", F.lit("/"), "status"))
)

ranked = (
    base
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    ranked
    .withColumn("next_discount", F.lead("discount", 2).over(w2))
    .withColumn("shipping_fee_avg_5", F.avg("shipping_fee").over(w3))
)

with_dims = (
    with_windows
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.signup_date"), F.col("p.name"))
)

result = (
    with_dims
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("amount"))).alias("cancelled_total_amount"),
        F.corr("shipping_fee", "amount").alias("corr_shipping_fee_amount"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count_if(F.col("status") == "SHIPPED").alias("shipped_events"),
        F.sum(F.when(F.col("status") == "PENDING", F.col("discount")).otherwise(0)).alias("pending_discount"),
    )
    .orderBy(F.col("shipped_events").desc_nulls_first())
)
