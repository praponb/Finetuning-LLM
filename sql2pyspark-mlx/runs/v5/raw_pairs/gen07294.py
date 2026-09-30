from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.year("sale_date").alias("sale_year"))
    .agg(F.max("units").alias("max_units"))
    .filter(F.col("max_units") < 18)
    .select("sale_year", "max_units")
)
