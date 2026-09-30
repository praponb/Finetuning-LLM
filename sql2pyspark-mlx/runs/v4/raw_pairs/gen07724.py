from pyspark.sql import functions as F

flattened = df_carts.select("id", F.inline("items"))

result = (
    flattened
    .select("id", "product_id", "qty", "price", "promo")
    .orderBy(F.col("qty").asc_nulls_last())
)
