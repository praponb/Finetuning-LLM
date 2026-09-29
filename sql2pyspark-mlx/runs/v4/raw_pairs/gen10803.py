from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 5, 7, 10]))
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.max("units").alias("max_units"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_sales_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
        F.col("t.stddev_units"),
        F.col("t.max_units"),
    )
    .orderBy("store_name")
)
