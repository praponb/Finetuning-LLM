from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .groupBy(
        F.when(F.col("p.brand").isin(["Umbrella", "Acme"]), "group_a").otherwise("other").alias("brand_group"),
    )
    .agg(F.max(F.col("ss.units")).alias("max_units"))
    .filter(F.col("max_units") >= 29)
    .orderBy(F.col("brand_group").desc())
)
