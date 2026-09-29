from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") > 1200) & (F.col("amount") > 1300) & F.col("merchant").isNull())
    .cube(F.quarter("txn_time").alias("txn_quarter"), "txn_type")
    .agg(
        F.sum(F.when(F.col("merchant") == "Lazada", F.col("amount")).otherwise(0)).alias("lazada_amount"),
    )
    .orderBy(F.col("lazada_amount").desc())
)
