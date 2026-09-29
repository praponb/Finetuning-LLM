from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("store_id") == 1) & F.col("sale_date").between("2024-07-29", "2025-03-01") & (F.col("units") != 31))
    .withColumn("revenue_filled", F.coalesce("revenue", F.lit(0)))
    .orderBy(F.col("id").desc())
)
