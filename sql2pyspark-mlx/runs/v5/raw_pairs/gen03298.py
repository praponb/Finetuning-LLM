from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_orders
    .filter(F.col("discount").isNotNull() & (F.col("order_date") >= F.lit("2023-08-21").cast("date")))
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        F.round("shipping_fee", 0).alias("shipping_fee_rounded"),
        F.when(F.col("status") == "PENDING", 1).otherwise(0).alias("is_pending"),
    )
)

result = (
    valid
    .select(
        "id",
        "amount",
        "discount",
        "shipping_fee",
        "shipping_fee_rounded",
        F.min("discount").over(w).alias("last3_min_discount"),
        F.max("discount").over(w).alias("last3_max_discount"),
        F.sum("discount").over(w).alias("last3_sum_discount"),
    )
)
