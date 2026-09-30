from pyspark.sql import functions as F

result = (
    df_store_sales
    .withColumn("days_since_sale_date", F.datediff(F.current_date(), "sale_date"))
    .withColumn("revenue_per_unit", F.round((F.col("revenue") / F.col("units")), 2))
)
