from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "store_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_orders
    .filter(F.col("store_id").isNotNull() & F.col("channel").isNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "amount",
        "discount",
        F.greatest("discount", "amount").alias("greatest_discount_amount"),
        (F.col("shipping_fee") + F.col("amount")).alias("amount_with_shipping"),
    )
)

result = (
    valid
    .select(
        "*",
        F.sum("greatest_discount_amount").over(w).alias("running_sum_greatest_discount_amount"),
        F.min("greatest_discount_amount").over(w).alias("running_min_greatest_discount_amount"),
        F.avg("greatest_discount_amount").over(w).alias("running_avg_greatest_discount_amount"),
        F.count("greatest_discount_amount").over(w).alias("running_count_greatest_discount_amount"),
    )
)
