from pyspark.sql import functions as F

by_dimension = (
    df_order_items
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .rollup(F.col("p.brand").alias("brand"))
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("e.unit_price")).alias("avg_unit_price"),
        F.min(F.col("e.quantity")).alias("min_quantity"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    by_dimension
    .select("num_rows", "min_quantity", "gid")
    .orderBy(F.col("num_rows").asc_nulls_last(), "min_quantity")
)
