from pyspark.sql import functions as F

src = df_products.filter((F.col("cost") >= 430) & (F.col("stock_qty") > 332))

category_summary = (
    src
    .groupBy("category")
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
)

result = (
    category_summary
    .filter(F.col("stddev_cost") < 75)
    .select("category", "min_launch_date", "stddev_cost")
)
