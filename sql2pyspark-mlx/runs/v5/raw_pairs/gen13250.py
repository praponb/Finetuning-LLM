from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.month("launch_date").alias("launch_month"))
    .agg(
        F.max("cost").alias("max_cost"),
        F.max("price").alias("max_price"),
    )
    .filter(F.col("max_price") > 470)
    .select("launch_month", "max_cost", "max_price")
)
