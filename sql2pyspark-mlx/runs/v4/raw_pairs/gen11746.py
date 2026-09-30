from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") >= 1180)
    .select("launch_date")
    .union(df_stores.select("opened_date"))
    .distinct()
)
