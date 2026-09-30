from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date")
    .orderBy("launch_date")
)
