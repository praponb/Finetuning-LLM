from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("revenue", F.lit("store_sale").alias("source"))
    .union(df_payments.select("amount", F.lit("payment").alias("source")))
    .distinct()
    .orderBy("source")
)
