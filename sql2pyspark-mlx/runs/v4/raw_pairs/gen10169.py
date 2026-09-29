from pyspark.sql import functions as F

result = (
    df_orders
    .select("status", F.lit("order").alias("source"))
    .unionByName(
        df_accounts
        .filter(F.year("opened_date") >= 2025)
        .select("status", F.lit("account").alias("source"))
    )
    .distinct()
    .orderBy(F.col("source").desc())
)
