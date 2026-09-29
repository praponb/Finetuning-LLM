from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([4, 5, 12]))
    .select("tags")
)
