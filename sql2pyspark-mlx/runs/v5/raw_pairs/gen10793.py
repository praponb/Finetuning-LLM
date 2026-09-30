from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .filter(F.col("products.product_name").like("%Lapto%"))
    .groupBy(
        F.when(F.col("suppliers.rating") >= 4.1, "high").when(F.col("suppliers.rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("products.brand")))).alias("brand_values"),
        F.sort_array(F.collect_set(F.col("products.brand"))).alias("brand_list"),
    )
)
