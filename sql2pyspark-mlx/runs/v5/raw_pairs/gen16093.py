from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .filter(F.col("units").between(19, 27))
    .groupBy("product_id")
    .agg(F.round(F.stddev("units"), 2).alias("stddev_units"))
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("ssbp"), F.col("p.id") == F.col("ssbp.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("p.price"),
        F.col("ssbp.stddev_units"),
    )
    .orderBy(F.col("id").desc(), "price")
)
