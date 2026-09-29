from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date")
    .union(df_stores.select("opened_date"))
    .distinct()
)
