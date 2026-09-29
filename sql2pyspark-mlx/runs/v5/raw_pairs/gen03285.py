from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("price") < 570) | ~F.col("brand").isin(["Umbrella", "Initech", "Acme"]))
    .select("id", "brand", "price", F.round("cost", 1).alias("cost_rounded"))
)
