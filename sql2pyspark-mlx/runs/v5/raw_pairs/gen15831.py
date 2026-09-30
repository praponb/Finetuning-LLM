from pyspark.sql import functions as F

result = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("dim"), F.col("a.supplier_id") == F.col("dim.id"), "inner")
    .rollup(F.col("dim.city").alias("city"), F.col("a.category").alias("category"))
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("a.stock_qty")).alias("max_stock_qty"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("max_stock_qty").desc_nulls_first())
)
