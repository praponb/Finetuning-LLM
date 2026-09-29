from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").isNotNull())
    .select("status", "customer_id")
    .subtract(
        df_orders
        .filter(F.col("shipping_fee").between(13, 17))
        .select("status", "customer_id")
    )
)
