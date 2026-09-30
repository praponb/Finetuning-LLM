from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("brand").isin(["Stark", "Umbrella"]))
    .select("id", "price", "product_name", F.explode("tags").alias("label"))
)
