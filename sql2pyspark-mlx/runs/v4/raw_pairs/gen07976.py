from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") <= 3800) & (F.col("sale_date") > "2025-02-01"))
    .groupBy(
        "store_id",
        F.year("sale_date").alias("sale_year"),
        F.when(F.col("units") >= 45, "high").when(F.col("units") >= 13, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(F.max("sale_date").alias("latest_sale_date"))
)
