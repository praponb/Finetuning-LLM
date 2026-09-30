from pyspark.sql import functions as F

src = df_products.filter(F.col("supplier_id") == 8)

agg = (
    src
    .groupBy("supplier_id")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.min("launch_date").alias("first_launch_date"),
    )
)

result = agg.select("supplier_id", "first_launch_date")
