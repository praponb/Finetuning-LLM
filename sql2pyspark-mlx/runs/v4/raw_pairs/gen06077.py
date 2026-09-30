from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "PENDING")
    .select("customer_id", "store_id")
    .unionByName(
        df_orders
        .filter(F.col("channel") == "mobile")
        .select("customer_id", "store_id")
    )
    .distinct()
)
