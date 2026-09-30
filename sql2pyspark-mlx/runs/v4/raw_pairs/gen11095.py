from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([9, 24, 34, 40]))
    .select("amount")
    .intersect(df_products.select("price"))
)
