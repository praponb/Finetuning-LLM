from pyspark.sql import functions as F

result = (
    df_products
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id") == F.col("d.id"), "inner")
    .cube(F.col("d.country").alias("country"), F.col("e.category").alias("category"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("e.cost")).alias("min_cost"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("cnt").asc_nulls_first())
)
