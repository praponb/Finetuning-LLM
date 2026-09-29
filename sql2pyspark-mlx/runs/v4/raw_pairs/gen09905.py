from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .pivot("merchant", ["Lazada", "Amazon"])
    .agg(F.max("amount").alias("amount"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "txn_type",
        F.col("Lazada_amount").alias("lazada_amount"),
        F.col("Lazada_cnt").alias("lazada_cnt"),
        F.col("Amazon_amount").alias("amazon_amount"),
        F.col("Amazon_cnt").alias("amazon_cnt"),
    )
)
