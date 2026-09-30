from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .groupBy(F.col("c.country"), F.col("c.city"))
    .agg(F.count(F.when(F.col("a.status") == "OPEN", 1)).alias("open_count"))
    .filter(F.col("open_count") >= 4)
    .orderBy("city", F.col("open_count").desc(), "country")
    .limit(50)
)
