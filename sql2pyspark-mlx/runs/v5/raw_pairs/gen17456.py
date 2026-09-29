from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "SHIPPED")
    .select("channel", "store_id")
    .unionByName(
        df_orders
        .filter(F.col("customer_id").isin([20, 27, 39]))
        .select("channel", "store_id")
    )
    .distinct()
)
