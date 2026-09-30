from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["partner", "web"]))
    .select("store_id", "order_date")
    .subtract(
        df_orders
        .filter(F.col("status").isin(["RETURNED", "COMPLETED"]))
        .select("store_id", "order_date")
    )
    .orderBy("order_date")
)
