from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .pivot("txn_type", ["CREDIT", "DEBIT"])
    .agg(F.sum("amount").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "merchant",
        F.col("CREDIT_value_sum").alias("credit_value_sum"),
        F.col("CREDIT_item_count").alias("credit_item_count"),
        F.col("DEBIT_value_sum").alias("debit_value_sum"),
        F.col("DEBIT_item_count").alias("debit_item_count"),
    )
)
