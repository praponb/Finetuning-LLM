from pyspark.sql import functions as F

result = (
    df_orders
    .select("customer_id", "status")
    .exceptAll(
        df_orders
        .filter(F.col("status").isin(["SHIPPED", "RETURNED"]))
        .select("customer_id", "status")
    )
)
