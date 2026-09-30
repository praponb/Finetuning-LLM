from pyspark.sql import functions as F

result = (
    df_products
    .select("category", "launch_date")
    .orderBy(F.col("launch_date").desc(), "category")
)
