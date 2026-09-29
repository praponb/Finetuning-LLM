from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.col("ss.revenue") <= 4000)
    .groupBy(
        F.when(F.col("p.brand").isin(["Globex", "Stark"]), "core").otherwise("other").alias("brand_group"),
        F.col("p.brand"),
    )
    .agg(F.max(F.col("ss.units")).alias("max_units"))
    .filter(F.col("max_units") >= 29)
    .orderBy("brand")
)
