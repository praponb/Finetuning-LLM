from pyspark.sql import functions as F

dim_totals = (
    df_inventory
    .alias("s")
    .join(df_products.alias("dim"), F.col("s.product_id") == F.col("dim.id"), "inner")
    .cube(F.col("dim.category").alias("category"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("s.quantity")), 2).alias("avg_quantity"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    dim_totals
    .select(
        "category",
        "cnt",
        "gid",
        F.when(F.col("category").rlike("^(Fashion|Sports)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("category_zone"),
    )
    .orderBy(F.col("category").desc_nulls_last())
)
