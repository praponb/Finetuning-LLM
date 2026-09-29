from pyspark.sql import functions as F

result = (
    df_products
    .select("cost", F.lit("product").alias("record_type"))
    .union(df_orders.select("amount", F.lit("order").alias("record_type")))
    .orderBy("record_type")
)
