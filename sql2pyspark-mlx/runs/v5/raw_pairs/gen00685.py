from pyspark.sql import functions as F

result = (
    df_orders
    .select("store_id")
    .exceptAll(
        df_orders
        .filter(F.col("channel") == "store")
        .select("store_id")
    )
)
