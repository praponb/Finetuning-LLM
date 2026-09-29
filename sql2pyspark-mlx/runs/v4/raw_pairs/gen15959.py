from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Umbrella", "Acme", "Initech"]))
    .select("id", "product_name", F.explode("tags").alias("label"))
)
