from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter((F.col("price") >= 1170) & F.array_contains("tags", "bestseller") & F.col("product_name").isNull())
    .withColumn("price_int", F.col("price").cast("int"))
)

launch_month_summary = (
    recent_products
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"), "category")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
    )
)

result = launch_month_summary.select("launch_month", "category", "category_values")
