from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("stock_qty") < 179) & F.col("supplier_id").isin([10, 12]) & F.col("product_name").isNotNull())
    .groupBy("supplier_id", "brand")
    .agg(
        F.max("price").alias("max_price"),
        F.sum("stock_qty").alias("total_stock_qty"),
    )
)
