from pyspark.sql import functions as F

filtered_customers = df_customers.filter(F.col("name").like("b%"))

summary = (
    filtered_customers
    .groupBy("city", "segment")
    .agg(
        F.round(F.stddev("age"), 2).alias("stddev_age"),
        F.round(((100 * F.sum(F.when(F.col("segment") == "Consumer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_consumer"),
    )
)

result = summary.filter(F.col("stddev_age") < 47)
