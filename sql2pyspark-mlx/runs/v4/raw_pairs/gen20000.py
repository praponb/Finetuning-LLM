from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Electronics", "Grocery", "Fashion"]))
    .select("id", "brand")
    .unionByName(
        df_products
        .filter(F.lower("product_name").like("%ack%"))
        .select("id", "brand")
    )
)
