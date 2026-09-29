from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([20, 30, 32]))
    .select("order_date", F.lit("order").alias("source"))
    .unionByName(
        df_accounts
        .select(F.col("opened_date").alias("order_date"), F.lit("account").alias("source"))
    )
    .distinct()
    .orderBy(F.col("source").desc())
)
