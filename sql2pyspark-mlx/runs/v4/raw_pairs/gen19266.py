from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id")
w4 = Window.partitionBy("status").orderBy(F.col("order_date").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("status").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("customer_id")
w8 = Window.partitionBy("store_id").orderBy("amount", "id").rowsBetween(-3, -1)
w9 = Window.partitionBy("store_id").orderBy("corr_shipping_fee_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("store_id", "customer_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_orders
    .withColumn("discount_avg_5_prior", F.avg("discount").over(w))
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w2))
    .withColumn("corr_shipping_fee_amount", F.corr("shipping_fee", "amount").over(w3))
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.rank().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_last = (
    deduped
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"), F.col("c.unit_price"))
)

result = (
    with_last
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w6))
    .withColumn("quantity_share", F.round((F.col("quantity") / F.sum("quantity").over(w7)), 4))
    .withColumn("quantity_avg_3_prior", F.avg("quantity").over(w8))
    .withColumn("last_known_discount", F.last("discount", ignorenulls=True).over(w9))
    .withColumn("running_shipping_fee", F.sum("shipping_fee").over(w10))
)
