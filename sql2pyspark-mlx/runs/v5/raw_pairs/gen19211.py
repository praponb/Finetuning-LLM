from pyspark.sql import functions as F

by_dimension = (
    df_inventory
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .cube(F.col("p.brand").alias("brand"), F.col("p.category").alias("category"))
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("src.quantity")).alias("max_quantity"),
    )
)

result = (
    by_dimension
    .select(
        "brand",
        "category",
        "max_quantity",
        F.when(F.col("brand").rlike("^(Stark|Globex)$"), "GROUP_B").otherwise("EXTENDED").alias("brand_zone"),
    )
    .orderBy(F.col("brand_zone").asc_nulls_first())
)
