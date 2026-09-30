from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "country").orderBy(F.col("age").asc_nulls_last())

deduped = (
    df_customers
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .groupBy("country")
    .agg(
        F.corr("credit_limit", "age").alias("corr_credit_limit_age"),
        F.avg("credit_limit").alias("avg_credit_limit"),
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("credit_limit_per_london"),
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("age_per_london"),
    )
)
