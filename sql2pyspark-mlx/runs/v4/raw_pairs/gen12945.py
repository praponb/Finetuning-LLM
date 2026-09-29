from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .groupBy(
        F.when(F.col("suppliers.rating") >= 4.3, "high").when(F.col("suppliers.rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
        F.col("suppliers.country"),
    )
    .agg(
        F.max(F.col("store_sales.sale_date")).alias("latest_sale_date"),
        F.round(F.avg(F.col("store_sales.units")), 2).alias("avg_units"),
    )
    .orderBy(F.col("avg_units").desc())
)
