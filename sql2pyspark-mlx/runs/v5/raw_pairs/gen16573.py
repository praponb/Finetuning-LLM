from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") >= 14)
    .withColumn("revenue_filled", F.coalesce("revenue", F.lit(0)))
    .withColumn("revenue_per_unit", (F.col("revenue") * F.col("units")))
    .orderBy(F.col("product_id").desc())
)
