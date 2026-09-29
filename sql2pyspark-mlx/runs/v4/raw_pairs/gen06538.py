from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Electronics", "Sports", "Books", "Home"])
    .agg(F.avg("price"))
)
