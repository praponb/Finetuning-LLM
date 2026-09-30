from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([25, 31, 36]))
    .select("amount")
    .union(df_shipments.filter(F.col("order_id").isNotNull()).select("cost"))
    .orderBy("amount")
)
