from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["DEBIT", "REFUND"]))
    .select(F.col("amount").alias("value"))
    .unionByName(
        df_order_items
        .filter(F.col("unit_price").between(170, 370))
        .select(F.col("unit_price").alias("value"))
    )
    .distinct()
)
