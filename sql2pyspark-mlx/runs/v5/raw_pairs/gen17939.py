from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "inner")
    .groupBy(
        F.when(F.col("suppliers.country").isin(["DE", "US"]), "group_a").otherwise("other").alias("country_group"),
        F.when(F.col("suppliers.rating") >= 3.5, "high").when(F.col("suppliers.rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.countDistinct(F.col("products.stock_qty")).alias("unique_categorys"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("products.brand")))).alias("brand_values"),
    )
    .orderBy("brand_values", "rating_bucket")
)
