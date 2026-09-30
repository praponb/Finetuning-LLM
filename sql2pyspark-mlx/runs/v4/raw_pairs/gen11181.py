from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", "brand")
    .agg(F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_count"))
    .filter(F.col("globex_count") < 4)
    .orderBy("brand", F.col("globex_count").desc())
)
