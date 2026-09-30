from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(F.col("s.sqft").between(6784, 14604))
    .select(
        F.col("o.status"),
        F.col("o.channel"),
        F.col("o.store_id"),
        F.col("s.region"),
    )
    .orderBy(F.col("channel").desc(), "region")
)
