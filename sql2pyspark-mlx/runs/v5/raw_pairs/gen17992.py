from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter((F.col("balance") >= 60000) & (F.col("opened_date") < "2023-06-17") & (F.col("balance") <= 68000))
    .withColumn("balance_int", F.col("balance").cast("int"))
)

agg = (
    recent_accounts
    .groupBy(F.year("opened_date").alias("opened_year"), "customer_id")
    .agg(
        F.count("balance").alias("balance_count"),
        F.countDistinct("balance_int").alias("unique_statuss"),
    )
)

result = agg.select("opened_year", "customer_id", "balance_count", "unique_statuss")
