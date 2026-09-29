from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Fashion", "Sports"]))
    .select("brand")
    .distinct()
)
