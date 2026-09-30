from pyspark.sql import functions as F

filtered_products = (
    df_products
    .filter((F.col("supplier_id").isNotNull() & (F.col("stock_qty") > 227)) | F.col("cost").between(340, 600))
)

summary = (
    filtered_products
    .groupBy("supplier_id")
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
)

result = summary.select("supplier_id", "stddev_cost")
