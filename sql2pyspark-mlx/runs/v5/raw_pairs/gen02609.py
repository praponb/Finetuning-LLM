from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .groupBy(
        F.when(F.col("suppliers.rating") >= 4.1, "high").when(F.col("suppliers.rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count("*").alias("num_products"),
        (F.max(F.col("products.stock_qty")) - F.min(F.col("products.stock_qty"))).alias("stock_qty_range"),
    )
)
