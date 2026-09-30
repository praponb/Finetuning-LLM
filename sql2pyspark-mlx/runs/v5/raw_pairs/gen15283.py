from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "city").orderBy(F.col("signup_date").desc_nulls_last(), "id")

latest = (
    df_customers
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .groupBy(F.when(F.col("age") >= 63, "LARGE").otherwise("SMALL").alias("age_tier"))
    .agg(
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("city") == "Bangkok", 1)), F.lit(0)), 2).alias("age_per_bangkok"),
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("segment") == "Corporate", 1)), F.lit(0)), 2).alias("age_per_corporate"),
        F.corr("age", "credit_limit").alias("corr_age_credit_limit"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
    )
    .orderBy("age_per_bangkok")
)
