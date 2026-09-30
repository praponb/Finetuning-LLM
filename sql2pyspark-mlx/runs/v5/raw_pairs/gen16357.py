from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("order_date") >= "2024-05-01") & F.col("store_id").isNotNull())
    .withColumn("discount_times_shipping_fee", (F.col("discount") * F.col("shipping_fee")))
    .orderBy("customer_id", "id")
    .limit(20)
)
