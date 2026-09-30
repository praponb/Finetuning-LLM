from pyspark.sql import functions as F

result = (
    df_accounts
    .select("opened_date", F.lit("account").alias("kind"))
    .union(df_products.select("launch_date", F.lit("product").alias("kind")))
    .orderBy("opened_date")
)
