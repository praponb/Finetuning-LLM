from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .filter(F.col("units") < 23)
    .groupBy("product_id")
    .agg(
        (F.max("units") - F.min("units")).alias("units_range"),
        F.avg("units").alias("avg_units"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(
        F.col("p.supplier_id"),
        F.col("p.brand"),
        F.col("t.units_range"),
        F.col("t.avg_units"),
    )
    .orderBy("avg_units")
)
