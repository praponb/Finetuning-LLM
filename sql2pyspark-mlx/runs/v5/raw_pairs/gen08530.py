from pyspark.sql import functions as F

filtered = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-05-20").cast("date")) & (~F.col("status").eqNullSafe(F.lit("OPEN"))))
    .withColumn("balance_int", F.col("balance").cast("int"))
)

result = (
    filtered
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.city").alias("city"), F.col("c.segment").alias("segment"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("e.balance")).alias("max_balance"),
        F.round(F.avg(F.col("e.balance_int")), 2).alias("avg_balance_int"),
    )
    .orderBy(F.col("cnt").desc())
)
