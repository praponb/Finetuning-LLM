from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").isNull())
    .select("id", "channel")
    .unionByName(
        df_orders
        .filter(F.col("store_id").isin([15, 19, 28, 37]))
        .select("id", "channel")
    )
    .distinct()
    .orderBy(F.col("id").desc())
)
