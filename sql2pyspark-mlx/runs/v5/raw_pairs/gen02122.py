from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "store_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id", "status").orderBy("status_length", "id").rowsBetween(-5, -1)

cleaned = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        F.length("status").alias("status_length"),
        F.when(F.col("discount") >= 0.35, "High").when(F.col("discount") >= 0.08, "Medium").otherwise("Low").alias("discount_tier"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "store_id",
        "status",
        "amount",
        "discount",
        "status_length",
        "discount_tier",
        F.last("shipping_fee", ignorenulls=True).over(w).alias("last_known_shipping_fee"),
        F.sum("shipping_fee").over(w2).alias("running_shipping_fee"),
        F.avg("shipping_fee").over(w3).alias("shipping_fee_avg_5_prior"),
    )
)
