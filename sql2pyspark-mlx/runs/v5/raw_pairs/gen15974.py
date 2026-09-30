from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.alias("products"), F.col("suppliers.id") == F.col("products.supplier_id"), "left")
    .groupBy(F.col("suppliers.city"))
    .agg(
        F.count(F.col("products.id")).alias("product_count"),
        F.sum(F.when(F.col("products.category") == "Sports", 1).otherwise(0)).alias("sports_count"),
        F.round(F.stddev(F.col("products.stock_qty")), 2).alias("stddev_stock_qty"),
    )
    .orderBy(F.col("stddev_stock_qty").desc(), F.col("product_count").desc(), "city")
    .limit(10)
)
