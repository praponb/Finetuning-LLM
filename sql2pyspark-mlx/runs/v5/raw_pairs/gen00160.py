from pyspark.sql import functions as F

filtered_customers = df_customers.filter(F.col("name").like("Ka%"))

summary = (
    filtered_customers
    .groupBy(
        F.when(F.col("country").isin(["TH", "US"]), "group_a").otherwise("other").alias("country_group"),
    )
    .agg(
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
        F.max("age").alias("max_age"),
    )
)

result = (
    summary
    .filter(F.col("max_age") > 59)
    .select("country_group", "consumer_count", "max_age")
    .orderBy(F.col("country_group").desc())
)
