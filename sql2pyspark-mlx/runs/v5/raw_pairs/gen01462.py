from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.min("revenue").alias("min_revenue"))
)

result = (
    df_stores.alias("stores")
    .join(store_totals.alias("t"), F.col("stores.id") == F.col("t.store_id"), "left")
    .select(
        F.col("stores.id"),
        F.col("stores.sqft"),
        F.col("stores.store_name"),
        F.col("t.min_revenue"),
    )
    .orderBy(F.col("sqft").desc(), "store_name")
)
