from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select("status", "channel")
    .unionByName(
        df_orders
        .filter(F.col("store_id").isNotNull())
        .select("status", "channel")
    )
    .distinct()
)
