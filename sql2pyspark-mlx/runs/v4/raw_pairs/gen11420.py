from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("signup_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("country").orderBy(F.col("age").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())

valid = (
    df_customers
    .filter(F.col("signup_date") > F.lit("2023-03-06").cast("date"))
    .withColumn("segment_upper", F.upper("segment"))
    .withColumn("signup_date_weekofyear", F.weekofyear("signup_date"))
    .withColumn("city_filled", F.coalesce("city", F.lit("N/A")))
)

enriched = (
    valid
    .withColumn("signup_date_weekofyear_avg_3", F.avg("signup_date_weekofyear").over(w))
    .withColumn("event_seq", F.row_number().over(w2))
)

latest_match = (
    enriched
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.end_date").alias("subscription_end_date"))
)

result = (
    latest_match
    .groupBy(
        F.when(F.col("credit_limit") >= 13200, "HIGH").otherwise("LOW").alias("credit_limit_tier"),
        F.when(F.col("age") >= 68, "HIGH").otherwise("LOW").alias("age_tier"),
    )
    .agg(
        F.avg(F.when(F.col("country") == "US", F.col("credit_limit"))).alias("us_avg_credit_limit"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("city") == "London", F.col("age")).otherwise(0)).alias("london_age"),
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("country") == "SG", 1)), F.lit(0)), 2).alias("event_seq_per_sg"),
        F.count_if(F.col("segment") == "Corporate").alias("corporate_events"),
    )
)
