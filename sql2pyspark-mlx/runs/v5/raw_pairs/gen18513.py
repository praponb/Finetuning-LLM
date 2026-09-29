from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") > 33)
    .select("id", "units", F.date_add("sale_date", 86).alias("sale_date_plus_86d"))
)
