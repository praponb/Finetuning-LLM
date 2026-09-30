from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") >= "2023-09-01")
    .select("customer_id", "order_date")
    .unionByName(
        df_orders
        .filter(F.col("store_id").isin([17, 28, 39]))
        .select("customer_id", "order_date")
    )
    .orderBy("customer_id")
)
