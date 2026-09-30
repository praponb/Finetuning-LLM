from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("total_amount") >= 22000)
    .drop("total_amount")
)
