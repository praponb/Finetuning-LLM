from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Electronics", "Books", "Fashion", "Grocery"])
    .agg(F.max("price"))
)
