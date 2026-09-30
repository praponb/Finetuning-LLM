from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc_nulls_last())
w4 = Window.partitionBy("store_id")
w5 = Window.partitionBy("channel", "store_id").orderBy("amount", "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w7 = Window.partitionBy("store_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("discount", "id")

eligible = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.product_id").isin([15, 16])), "left_anti")
)

scored = (
    eligible
    .select(
        "*",
        F.avg("discount").over(w).alias("discount_avg_2_prior"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
        F.cume_dist().over(w3).alias("shipping_fee_cume_dist"),
        F.count("discount").over(w4).alias("store_id_count_discount"),
        F.lag("amount", 2).over(w5).alias("prev_amount"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.order_id").isNull()), "left_semi")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price").alias("order_item_unit_price"))
)

with_windows = (
    with_last
    .select(
        "*",
        F.first("amount", ignorenulls=True).over(w7).alias("first_known_amount"),
        F.lead("shipping_fee_cume_dist", 1).over(w8).alias("next_shipping_fee_cume_dist"),
        F.max("prev_amount").over(w2).alias("customer_id_max_prev_amount"),
    )
)

result = (
    with_windows
    .groupBy("channel", "customer_id")
    .pivot("status", ["COMPLETED", "RETURNED", "SHIPPED"])
    .agg(F.sum("prev_amount").alias("total"))
)
