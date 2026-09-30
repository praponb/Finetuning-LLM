from pyspark.sql import functions as F

result = (
    df_store_sales
    .withColumn("revenue_per_unit", F.round((F.col("units") / F.col("revenue")), 2))
    .withColumn("sale_date_label", F.date_format("sale_date", "yyyy-MM-dd"))
)
