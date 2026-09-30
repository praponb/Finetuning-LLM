from pyspark.sql import functions as F

src = df_customers.filter(F.month("signup_date").isin([1, 2, 9]))

summary = (
    src
    .groupBy("country", "segment")
    .agg(
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
        F.round(F.avg("age"), 2).alias("avg_age"),
    )
)

result = summary.select("country", "corporate_count", "avg_age")
