from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") <= 1360)
    .select("amount")
    .union(df_products.filter(F.col("cost") > 350).select("price"))
)
