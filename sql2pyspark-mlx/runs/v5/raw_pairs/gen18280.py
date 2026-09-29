from pyspark.sql import functions as F

dim_totals = (
    df_inventory
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.brand").alias("brand"), F.col("p.category").alias("category"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.quantity")).alias("max_quantity"),
    )
)

result = (
    dim_totals
    .select("brand", "category", "num_rows", "max_quantity")
    .orderBy(F.col("brand").desc_nulls_first(), F.col("num_rows").desc())
)
