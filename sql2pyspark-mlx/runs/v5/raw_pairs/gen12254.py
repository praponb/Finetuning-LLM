from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([2, 4, 10]))
    .select("customer_id", "channel")
    .unionByName(
        df_orders
        .filter(F.col("shipping_fee") >= 26)
        .select("customer_id", "channel")
    )
    .distinct()
)
