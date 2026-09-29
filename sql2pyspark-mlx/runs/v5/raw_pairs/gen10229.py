from pyspark.sql import functions as F

src = df_store_sales.filter(F.col("sale_date") <= "2025-04-16")

agg = (
    src
    .groupBy("product_id")
    .agg(
        F.sum("units").alias("total_units"),
        F.avg("revenue").alias("avg_revenue"),
    )
)

result = agg.filter(F.col("total_units") < 130)
