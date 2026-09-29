from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("ss.units").between(19, 28))
    .groupBy(F.col("s.city"))
    .agg(
        F.avg(F.col("ss.units")).alias("avg_units"),
        F.countDistinct(F.col("ss.units")).alias("unique_stores"),
    )
    .orderBy("city")
)
