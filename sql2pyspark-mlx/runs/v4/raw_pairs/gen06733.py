from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("age", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("city", "country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("city", "segment").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy(F.col("de_avg_credit_limit_avg_6_prior").desc())

metrics = (
    df_customers
    .select(
        "*",
        F.avg("credit_limit").over(w).alias("credit_limit_avg_6_prior"),
        F.sum("age").over(w2).alias("cumulative_age"),
        F.sum("credit_limit").over(w_part).alias("running_credit_limit"),
    )
)

result = (
    metrics
    .groupBy("country", "segment")
    .agg(
        F.round(F.sum("running_credit_limit") / F.nullif(F.count(F.when(F.col("segment") == "Corporate", 1)), F.lit(0)), 2).alias("running_credit_limit_per_corporate"),
        F.avg(F.when(F.col("country") == "DE", F.col("credit_limit_avg_6_prior"))).alias("de_avg_credit_limit_avg_6_prior"),
        F.percentile_approx("running_credit_limit", 0.95).alias("running_credit_limit_p95"),
        F.sum(F.when(F.col("segment") == "Corporate", F.col("credit_limit_avg_6_prior")).otherwise(0)).alias("corporate_credit_limit_avg_6_prior"),
    )
    .withColumn("de_avg_credit_limit_avg_6_prior_rank", F.rank().over(w4))
)
