from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") < "2023-10-01")
    .select("revenue", "store_id", "units", F.round("units", 1).alias("units_rounded"))
    .orderBy(F.col("units_rounded").desc())
)
