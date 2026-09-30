from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("credit_limit_tier").orderBy(F.col("age_stddev").desc())

result = (
    df_customers
    .groupBy(
        F.when(F.col("credit_limit") >= 15700, "HIGH").otherwise("LOW").alias("credit_limit_tier"),
        "country",
    )
    .agg(
        F.stddev_samp("credit_limit").alias("credit_limit_stddev"),
        F.stddev_samp("age").alias("age_stddev"),
        F.bit_or("age").alias("age_bit_or"),
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
    )
    .filter(F.col("age_stddev") >= 0)
    .withColumn("age_stddev_rank", F.rank().over(w))
    .orderBy(F.col("credit_limit_tier").desc())
)
