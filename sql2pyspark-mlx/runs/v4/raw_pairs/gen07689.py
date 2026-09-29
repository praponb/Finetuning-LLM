from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") < 830)
    .select("tags", "launch_date")
    .orderBy("launch_date")
)
