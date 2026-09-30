from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") < "2024-12-28")
    .select("category")
    .distinct()
)
