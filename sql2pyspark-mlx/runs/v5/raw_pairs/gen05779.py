from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("status")
w_grp = Window.partitionBy("store_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_orders
    .withColumn("discount_avg_2", F.avg("discount").over(w))
    .withColumn("corr_discount_shipping_fee", F.corr("discount", "shipping_fee").over(w2))
    .withColumn("first_known_amount", F.first("amount", ignorenulls=True).over(w_grp))
    .withColumn("running_amount", F.sum("amount").over(w4))
    .withColumn("running_discount", F.sum("discount").over(w5))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("discount") >= 0.43, "LARGE").otherwise("SMALL").alias("discount_tier"),
        "store_id",
    )
    .agg(
        F.count_if(F.col("status") == "SHIPPED").alias("shipped_events"),
        F.sum(F.when(F.col("channel") == "partner", F.col("corr_discount_shipping_fee")).otherwise(0)).alias("partner_corr_discount_shipping_fee"),
        F.sum("running_discount").alias("total_running_discount"),
        F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_rows"),
        F.sum("discount_avg_2").alias("total_discount_avg_2"),
    )
)
