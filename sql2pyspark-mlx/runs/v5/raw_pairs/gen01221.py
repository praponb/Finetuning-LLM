from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("account_id") == 26) | F.col("merchant").isin(["7-Eleven", "Lazada"]))
    .groupBy("txn_type")
    .agg(
        F.max("amount").alias("max_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
    .orderBy("txn_type_values")
)
