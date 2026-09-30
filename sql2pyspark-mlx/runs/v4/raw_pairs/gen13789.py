from pyspark.sql import functions as F

result = (
    df_orders
    .select("status", F.lit("order").alias("source"))
    .unionByName(
        df_accounts
        .filter(F.col("balance") <= 33000)
        .select("status", F.lit("account").alias("source"))
    )
    .distinct()
)
