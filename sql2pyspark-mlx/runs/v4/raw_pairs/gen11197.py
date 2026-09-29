from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "launch_date")
    .orderBy("id")
    .limit(20)
)
