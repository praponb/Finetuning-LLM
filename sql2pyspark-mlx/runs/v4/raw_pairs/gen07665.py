from pyspark.sql import functions as F

base = (
    df_customers
    .filter(~(F.col("country").isin(["TH", "US"]) & F.col("email").like("%@startup.io")))
    .withColumn("is_active_flag", F.col("is_active").cast("int"))
)

customers_stats = (
    base
    .groupBy("country")
    .agg(
        F.countDistinct("is_active_flag").alias("unique_countrys"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
    )
)

result = (
    customers_stats
    .orderBy(F.col("us_count").desc(), "country")
    .limit(5)
)
