from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Stark", "Initech"]))
    .select("id", "brand")
    .intersect(
        df_products
        .filter(F.col("category").isin(["Fashion", "Books", "Home"]))
        .select("id", "brand")
    )
)
