from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") >= 2300)
    .withColumn("sale_date_plus_months", F.add_months("sale_date", 3))
    .withColumn("units_filled", F.coalesce("units", F.lit(0)))
)
