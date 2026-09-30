from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") >= 960)
    .select("amount")
    .intersect(df_products.select("cost"))
    .orderBy("amount")
)
