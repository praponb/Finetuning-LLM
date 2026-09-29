from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("id", "units", "sale_date")
    .orderBy("units", F.col("id").desc())
    .limit(10)
)
