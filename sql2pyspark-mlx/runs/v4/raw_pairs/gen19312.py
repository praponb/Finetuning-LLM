from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([6, 7, 10]))
    .select("status", F.lit("account").alias("source"))
    .unionByName(
        df_payments
        .filter(F.col("method").isin(["bank_transfer", "cash", "paypal"]))
        .select("status", F.lit("payment").alias("source"))
    )
)
