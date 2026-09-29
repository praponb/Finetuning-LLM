from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.max("units").alias("max_units"))
)

result = (
    df_stores.alias("stores")
    .join(store_summary.alias("StoreSummary"), F.col("stores.id") == F.col("StoreSummary.store_id"), "left")
    .filter(F.col("StoreSummary.max_units") > 32)
    .select(
        F.col("stores.region"),
        F.col("stores.store_name"),
        F.col("StoreSummary.max_units"),
    )
)
