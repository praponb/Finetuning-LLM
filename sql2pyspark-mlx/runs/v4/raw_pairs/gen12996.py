from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("id", "units", "product_id", F.round("units", 1).alias("units_rounded"))
)
