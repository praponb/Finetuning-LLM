from pyspark.sql import functions as F

result = (
    df_store_sales
    .withColumn("revenue_rounded", F.round("revenue", 2))
    .withColumn("sale_date_plus_31d", F.date_add("sale_date", 31))
)
