from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").isNotNull() & F.month("sale_date").isin([1, 5, 8]) & F.col("store_id").isin([1, 4]))
    .withColumn("least_revenue_units", F.least("revenue", "units"))
    .withColumn("units_filled", F.coalesce("units", F.lit(0)))
)
