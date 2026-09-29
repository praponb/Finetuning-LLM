from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
        F.countDistinct("amount").alias("unique_accounts"),
    )
    .filter(F.col("stddev_amount") < 51)
    .orderBy(F.col("txn_type").desc(), F.col("unique_accounts").desc())
)
