from pyspark.sql import functions as F

result = (
    df_products
    .select("price", F.lit("product").alias("origin"))
    .union(df_payments.select("amount", F.lit("payment").alias("origin")))
)
