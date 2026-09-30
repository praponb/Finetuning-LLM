from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") > 0.31)
    .select("id", "customer_id", "store_id")
    .unionByName(
        df_orders
        .filter(F.col("status").isin(["PENDING", "SHIPPED"]))
        .select("id", "customer_id", "store_id")
    )
)
