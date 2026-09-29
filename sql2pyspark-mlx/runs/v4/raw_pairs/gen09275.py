from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.length("product_name") > 10)
    .groupBy("supplier_id")
    .agg(
        F.sum("cost").alias("total_cost"),
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
    )
    .filter(F.col("stddev_stock_qty") < 75)
    .select("supplier_id", "total_cost", "stddev_stock_qty")
    .orderBy(F.col("stddev_stock_qty").desc())
)
