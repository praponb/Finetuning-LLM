from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-04-01").cast("date")) & F.col("status").isNotNull())
    .select("*", F.col("balance").cast("int").alias("balance_int"))
)

result = (
    cleaned
    .alias("e")
    .join(df_customers.alias("dim"), F.col("e.customer_id") == F.col("dim.id"), "inner")
    .groupBy(
        F.col("dim.segment").alias("segment"),
        F.col("dim.city").alias("city"),
        F.col("e.account_type").alias("account_type"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("e.balance")).alias("max_balance"),
    )
)
