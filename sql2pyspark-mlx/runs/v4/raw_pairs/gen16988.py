from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_customers
    .select(
        "*",
        F.avg("credit_limit").over(w).alias("credit_limit_avg_3"),
        F.last("city", ignorenulls=True).over(w2).alias("last_known_city"),
    )
)

result = (
    with_windows
    .groupBy(
        "segment",
        F.when(F.col("age") >= 71, "HIGH").otherwise("LOW").alias("age_tier"),
    )
    .agg(
        F.avg("credit_limit").alias("avg_credit_limit"),
        F.max(F.when(F.col("country") == "JP", F.col("credit_limit_avg_3"))).alias("jp_max_credit_limit_avg_3"),
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("country") == "UK", 1)), F.lit(0)), 2).alias("age_per_uk"),
        F.percentile_approx("credit_limit_avg_3", 0.9).alias("credit_limit_avg_3_p90"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit_2"),
    )
)
