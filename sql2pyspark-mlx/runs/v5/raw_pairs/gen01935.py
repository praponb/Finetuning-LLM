from pyspark.sql import functions as F

src = df_customers.filter((F.length("name") > 11) | F.col("email").isNotNull())

customers_stats = (
    src
    .groupBy("segment")
    .agg(
        F.countDistinct("age").alias("unique_segments"),
        F.sum(F.when(F.col("segment") == "Consumer", F.col("credit_limit")).otherwise(0)).alias("consumer_credit_limit"),
    )
)

result = (
    customers_stats
    .select("segment", "unique_segments", "consumer_credit_limit")
    .orderBy(F.col("segment").desc())
    .limit(1)
)
