from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.year("sale_date") >= 2023)
    .groupBy(F.year("sale_date").alias("sale_year"))
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
    .filter(F.col("avg_units") < 26)
    .select("sale_year", "avg_revenue", "avg_units")
    .orderBy("avg_revenue")
)
