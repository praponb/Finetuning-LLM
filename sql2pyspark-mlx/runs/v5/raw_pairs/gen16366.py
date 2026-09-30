from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-07-04"))) & F.col("customer_id").isNotNull())
    .withColumn("opened_date_label", F.date_format("opened_date", "yyyy-MM-dd"))
    .withColumn("status_lower", F.lower("status"))
    .withColumn("status_clean", F.coalesce("status", F.lit("DIRECT")))
)

result = (
    valid
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupBy(
        F.col("c.segment").alias("segment"),
        F.col("a.account_type").alias("account_type"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("a.balance")), 2).alias("avg_balance"),
    )
)
