from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "store_id")
    .exceptAll(
        df_orders
        .filter(F.col("store_id").isin([7, 8, 21, 31]))
        .select("id", "store_id")
    )
)
