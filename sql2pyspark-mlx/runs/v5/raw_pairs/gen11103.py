from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("category").isin(["Fashion", "Electronics", "Sports"]))
    .select("id", "brand")
    .orderBy("brand", F.col("id").desc())
)
