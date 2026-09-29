from pyspark.sql import functions as F

result = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("dim"), F.col("a.supplier_id") == F.col("dim.id"), "left")
    .groupingSets(
        [["country", "category"], ["country"], []],
        F.col("dim.country").alias("country"),
        F.col("a.category").alias("category"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("a.cost")).alias("max_cost"),
        F.grouping_id().alias("gid"),
    )
)
