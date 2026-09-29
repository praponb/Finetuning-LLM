from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") > 37)
    .select("product_id")
    .unionByName(df_store_sales.filter(F.col("units") < 17).select("product_id"))
    .distinct()
    .orderBy("product_id")
)
