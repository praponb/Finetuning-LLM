from pyspark.sql import functions as F

filtered_products = (
    df_products
    .filter((F.length("product_name") >= 12) | (F.col("stock_qty") != 296))
    .withColumn("product_name_filled", F.coalesce("product_name", F.lit("Unknown")))
)

agg = (
    filtered_products
    .groupBy("brand")
    .agg(F.max("launch_date").alias("last_launch_date"))
)

result = agg.orderBy("brand", F.col("last_launch_date").desc())
