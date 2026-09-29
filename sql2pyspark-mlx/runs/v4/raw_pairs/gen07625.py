from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([14, 34]))
    .select("amount")
    .union(df_products.select("price"))
    .distinct()
    .orderBy("amount")
)
