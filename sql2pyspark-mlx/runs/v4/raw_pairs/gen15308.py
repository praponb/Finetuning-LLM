from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") > 0.27)
    .select("id", "channel", "customer_id")
    .unionByName(
        df_orders
        .filter(F.col("store_id").isin([10, 19, 27]))
        .select("id", "channel", "customer_id")
    )
)
