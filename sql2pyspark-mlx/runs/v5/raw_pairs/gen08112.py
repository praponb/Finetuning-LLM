from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") <= 4000) & (F.col("revenue").isNotNull() | F.col("revenue").isNull()))
    .withColumn("sale_date_quarter", F.quarter("sale_date"))
    .withColumn("revenue_rounded", F.round("revenue", 2))
)
