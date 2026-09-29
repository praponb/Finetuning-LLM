from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .pivot("merchant", ["Grab", "Amazon"])
    .agg(F.sum("amount").alias("amount"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "txn_type",
        F.col("Grab_amount").alias("grab_amount"),
        F.col("Grab_cnt").alias("grab_cnt"),
        F.col("Amazon_amount").alias("amazon_amount"),
        F.col("Amazon_cnt").alias("amazon_cnt"),
    )
)
