from pyspark.sql import functions as F

valid_customers = df_customers.filter(F.col("country") == "JP")

agg = (
    valid_customers
    .groupBy("country")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("segment") == "SMB", 1).otherwise(0))) / F.count("*")), 1).alias("pct_smb"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
    )
)

result = (
    agg
    .filter(F.col("consumer_count") < 6)
    .select("country", "consumer_count")
    .orderBy("country")
)
