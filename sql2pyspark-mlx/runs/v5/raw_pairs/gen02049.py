from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 4, 6]))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        (F.col("revenue") * F.col("units")).alias("revenue_per_unit"),
    )
)

result = (
    filtered
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .rollup(F.col("p.category").alias("category"))
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("e.units")).alias("avg_units"),
        F.min(F.col("e.revenue")).alias("min_revenue"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("avg_units").asc_nulls_last())
)
