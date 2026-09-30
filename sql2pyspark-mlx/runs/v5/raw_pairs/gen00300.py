from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(~F.col("p.product_name").like("C%"))
    .groupBy(F.col("p.brand"))
    .agg(
        F.min(F.col("ss.units")).alias("min_units"),
        F.countDistinct(F.col("ss.units")).alias("unique_stores"),
    )
    .orderBy("brand")
)
