from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .filter(F.col("pt.revenue_range") > 71)
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("pt.revenue_range"),
        F.col("pt.stddev_units"),
    )
    .orderBy(F.col("category").desc())
)
