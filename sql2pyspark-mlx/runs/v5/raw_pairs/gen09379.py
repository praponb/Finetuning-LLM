from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") <= 350)
    .select("brand")
    .distinct()
    .orderBy("brand")
)
