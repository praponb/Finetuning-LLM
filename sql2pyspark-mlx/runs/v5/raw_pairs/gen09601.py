from pyspark.sql import functions as F

result = (
    df_order_items
    .select("unit_price")
    .subtract(df_transactions.filter(F.col("is_flagged")).select("amount"))
)
