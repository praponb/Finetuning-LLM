from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_first())
w3 = Window.partitionBy("status").orderBy("discount")
w4 = Window.orderBy("order_date", "id")
w5 = Window.partitionBy("channel")
w6 = Window.partitionBy("store_id", "customer_id").orderBy("shipping_fee")
w7 = Window.partitionBy("store_id", "customer_id")
w8 = Window.partitionBy("customer_id").orderBy(F.col("corr_shipping_fee_amount").desc(), "id")
w9 = Window.partitionBy("status").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_orders
    .filter(F.col("channel").isNotNull())
    .withColumn("order_date_month_end", F.last_day("order_date"))
)

enriched = (
    base
    .withColumn("cumulative_amount", F.sum("amount").over(w))
    .withColumn("amount_percent_rank", F.percent_rank().over(w2))
    .withColumn("discount_rank", F.rank().over(w3))
    .withColumn("next_discount", F.lead("discount", 1).over(w4))
    .withColumn("corr_shipping_fee_amount", F.corr("shipping_fee", "amount").over(w5))
)

deduped = (
    enriched
    .withColumn("row_num", F.rank().over(w6))
    .filter(F.col("row_num") == 1)
)

filtered = (
    deduped
    .filter(F.col("channel") == "store")
    .select("*", F.floor("shipping_fee").alias("shipping_fee_whole"))
)

result = (
    filtered
    .select(
        "*",
        F.stddev_samp("amount_percent_rank").over(w7).alias("store_id_stddev_samp_amount_percent_rank"),
        F.ntile(4).over(w8).alias("corr_shipping_fee_amount_bucket"),
        F.last("next_discount", ignorenulls=True).over(w9).alias("last_known_next_discount"),
    )
)
