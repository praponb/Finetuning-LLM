from pyspark.sql import functions as F

result = (
    df_store_sales
    .withColumn("sale_date_plus_55d", F.date_add("sale_date", 55))
    .orderBy(F.col("store_id").desc(), F.col("sale_date_plus_55d").desc())
)
