from pyspark.sql import functions as F

src = df_products.filter(F.col("category") == "Fashion")

agg = (
    src
    .groupBy("brand")
    .agg(F.count(F.when(F.col("category") == "Home", 1)).alias("home_count"))
)

result = (
    agg
    .filter(F.col("home_count") > 8)
    .orderBy("home_count")
)
