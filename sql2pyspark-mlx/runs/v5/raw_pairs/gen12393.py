from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") > "2025-03-20")
    .withColumn("sale_date_plus_months", F.add_months("sale_date", 12))
)
