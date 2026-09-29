from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())

latest_match = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.start_date").alias("subscription_start_date"),
        F.col("c.monthly_fee"),
    )
)

result = (
    latest_match
    .groupBy(
        F.when(F.col("age") >= 61, "LARGE").otherwise("SMALL").alias("age_tier"),
        "country",
    )
    .agg(
        F.sum(F.when(F.col("country") == "SG", F.col("credit_limit")).otherwise(0)).alias("sg_credit_limit"),
        F.count_if(F.col("city") == "London").alias("london_events"),
        F.stddev_samp("age").alias("age_stddev"),
        F.percentile_approx("credit_limit", 0.9).alias("credit_limit_p90"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("monthly_fee_per_singapore"),
        F.size(F.collect_set("country")).alias("num_countries"),
    )
)
