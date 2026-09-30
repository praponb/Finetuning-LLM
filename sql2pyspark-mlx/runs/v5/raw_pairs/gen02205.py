from pyspark.sql import functions as F

result = (
    df_orders
    .select("customer_id", "channel")
    .subtract(
        df_orders
        .filter(F.col("store_id").isin([18, 36]))
        .select("customer_id", "channel")
    )
)
