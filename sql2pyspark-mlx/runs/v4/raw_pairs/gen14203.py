from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "channel").orderBy(F.col("order_date").desc(), "id")
w2 = Window.partitionBy("channel", "store_id").orderBy("latest_rank", "id")
w_recent = Window.orderBy(F.col("latest_rank").desc(), "id")
w4 = Window.partitionBy("status").orderBy(F.col("amount").desc())
w5 = Window.partitionBy("store_id").orderBy("order_date", "id")
w6 = Window.partitionBy("segment").orderBy(F.col("event_seq").desc(), "id")
w7 = Window.partitionBy("segment").orderBy("discount", "id")
w8 = Window.partitionBy("channel").orderBy(F.col("shipping_fee").desc_nulls_first())
w9 = Window.partitionBy("channel", "status").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_orders
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    latest
    .select(
        "*",
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w_recent).alias("event_seq"),
        F.percent_rank().over(w4).alias("amount_percent_rank"),
    )
)

enriched = (
    metrics
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.credit_limit"), F.col("p.segment"))
)

latest_orders = (
    enriched
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

latest_orders_2 = (
    latest_orders
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows = (
    latest_orders_2
    .withColumn("discount_bucket", F.ntile(3).over(w7))
    .withColumn("shipping_fee_percent_rank", F.percent_rank().over(w8))
    .withColumn("first_known_shipping_fee", F.first("shipping_fee", ignorenulls=True).over(w9))
)

result = (
    with_windows
    .groupBy("segment")
    .agg(
        F.countDistinct("channel").alias("distinct_channels"),
        F.size(F.collect_set("segment")).alias("num_segments"),
    )
    .orderBy(F.col("distinct_channels").desc_nulls_last())
)
