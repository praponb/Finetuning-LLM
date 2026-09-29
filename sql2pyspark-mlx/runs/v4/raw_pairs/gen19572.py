from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", "supplier_id")
    .pivot("brand", ["Umbrella", "Stark"])
    .agg(F.max("price").alias("value_sum"))
)
