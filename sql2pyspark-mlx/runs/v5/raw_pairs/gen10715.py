from pyspark.sql import functions as F

src = (
    df_products
    .filter(F.col("launch_date").between("2025-07-01", "2025-10-01") & (F.col("launch_date") < "2024-12-01") & (F.col("price") >= 1080))
)

brand_summary = (
    src
    .groupBy("brand")
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)

result = (
    brand_summary
    .filter(F.col("avg_cost") < 51)
    .orderBy(F.col("avg_cost").desc())
)
