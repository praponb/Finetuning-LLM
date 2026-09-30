from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") < 1700)
    .select("id", "units", "store_id", F.year("sale_date").alias("sale_date_year"))
    .orderBy("units", "id")
    .limit(20)
)
