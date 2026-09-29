from pyspark.sql import functions as F

src = (
    df_products
    .filter(F.col("brand").isin(["Stark", "Initech", "Acme"]) & (F.col("cost") < 600))
)

summary = (
    src
    .groupBy("category")
    .agg(F.sum("price").alias("total_price"))
)

result = (
    summary
    .orderBy("total_price", "category")
    .limit(10)
)
