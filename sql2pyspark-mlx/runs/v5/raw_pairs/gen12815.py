from pyspark.sql import functions as F

src = df_products.filter(F.col("supplier_id").isin([1, 29, 31, 39]))

launch_month_summary = (
    src
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .agg(F.max("launch_date").alias("last_launch_date"))
)

result = launch_month_summary.select("launch_month", "last_launch_date")
