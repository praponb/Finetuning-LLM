from pyspark.sql import functions as F

max_units = df_store_sales.agg(F.max("units")).first()[0]

result = (
    df_store_sales
    .filter(F.col("units") > max_units)
    .select("id", "units", "store_id")
    .orderBy(F.col("id").desc(), "store_id")
)
