from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status", F.lit("account").alias("source"))
    .unionByName(
        df_payments
        .filter(~F.col("method").isin(["card", "cash"]))
        .select("status", F.lit("payment").alias("source"))
    )
    .orderBy("source")
)
