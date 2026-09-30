from pyspark.sql import functions as F

result = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select("id", F.col("elem.product_id"), F.col("elem.qty"), F.col("elem.price"))
)
