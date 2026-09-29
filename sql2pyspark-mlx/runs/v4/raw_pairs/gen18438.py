from pyspark.sql import functions as F

result = (
    df_orders
    .select("customer_id")
    .exceptAll(
        df_orders
        .filter(F.col("channel").isin(["store", "partner", "web"]))
        .select("customer_id")
    )
)
