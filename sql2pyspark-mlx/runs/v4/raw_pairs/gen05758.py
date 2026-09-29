from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("shipping_fee", "id")
w2 = Window.partitionBy("status").orderBy("order_date", "id")
w3 = Window.orderBy("discount", "id")

metrics = (
    df_orders
    .select(
        "*",
        F.lag("shipping_fee", 1).over(w).alias("prev_shipping_fee"),
        F.lag("amount", 1).over(w2).alias("prev_amount"),
        F.ntile(4).over(w3).alias("discount_bucket"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("prev_shipping_fee") >= 35, "PREMIUM").otherwise("STANDARD").alias("prev_shipping_fee_tier"),
    )
    .agg(
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
        F.sum(F.when(F.col("channel") == "partner", F.col("discount_bucket")).otherwise(0)).alias("partner_discount_bucket"),
    )
    .filter(F.col("partner_count") > 1)
    .orderBy(F.col("prev_shipping_fee_tier").desc_nulls_last())
)
