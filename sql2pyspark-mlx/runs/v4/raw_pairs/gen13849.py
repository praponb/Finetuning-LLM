from pyspark.sql import functions as F

base = (
    df_products
    .filter((F.col("stock_qty") < 235) | F.col("brand").isNull())
    .withColumn("product_name_category_key", F.concat_ws(",", "product_name", "category"))
)

launch_year_summary = (
    base
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(F.max("launch_date").alias("last_launch_date"))
)

result = (
    launch_year_summary
    .select("launch_year", "last_launch_date")
    .orderBy(F.col("launch_year").desc())
    .limit(5)
)
