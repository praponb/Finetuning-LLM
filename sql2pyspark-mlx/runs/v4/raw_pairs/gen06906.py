from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter((F.col("category") != "Books") & F.col("brand").isin(["Initech", "Stark"]))
)

agg = (
    recent_products
    .groupBy("brand")
    .agg(
        F.sum(F.when(F.col("brand") == "Umbrella", F.col("price")).otherwise(0)).alias("umbrella_price"),
    )
)

result = (
    agg
    .orderBy(F.col("umbrella_price").desc(), "brand")
    .limit(10)
)
