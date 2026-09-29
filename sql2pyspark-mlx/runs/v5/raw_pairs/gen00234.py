from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("a")
    .join(df_stores.alias("dim"), F.col("a.store_id") == F.col("dim.id"), "left")
    .rollup(F.col("dim.city").alias("city"))
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("a.revenue")).alias("total_revenue"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("city").asc_nulls_first())
)
