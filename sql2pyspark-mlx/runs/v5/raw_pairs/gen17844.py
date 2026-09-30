from pyspark.sql import functions as F

by_dimension = (
    df_inventory
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["brand", "category"], ["brand"]],
        F.col("p.brand").alias("brand"),
        F.col("p.category").alias("category"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("s.quantity")).alias("min_quantity"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    by_dimension
    .select("brand", "category", "grouping_level")
    .orderBy(F.col("category").desc_nulls_first())
)
