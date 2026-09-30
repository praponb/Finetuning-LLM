from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id")
    .pivot("brand", ["Acme", "Globex"])
    .agg(F.sum("price").alias("amount"))
)
