from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id") == 12)
    .select("status")
    .unionByName(
        df_payments
        .filter(F.col("order_id").isin([11, 31, 33, 36]))
        .select("status")
    )
    .orderBy(F.col("status").desc())
)
