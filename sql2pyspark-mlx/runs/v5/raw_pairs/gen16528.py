from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("segment")
w2 = Window.orderBy(F.col("age").desc(), "id")
w_running = Window.partitionBy("city").orderBy(F.col("signup_date").desc(), "id")
w4 = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("country")

scored = (
    df_customers
    .withColumn("corr_age_credit_limit", F.corr("age", "credit_limit").over(w_part))
    .withColumn("age_bucket", F.ntile(4).over(w2))
    .withColumn("event_seq", F.row_number().over(w_running))
    .withColumn("credit_limit_avg_3_prior", F.avg("credit_limit").over(w4))
    .withColumn("country_min_credit_limit", F.min("credit_limit").over(w5))
)

result = (
    scored
    .groupBy(
        "city",
        F.when(F.col("credit_limit") >= 14600, "HIGH").otherwise("LOW").alias("credit_limit_tier"),
        F.date_trunc("month", "signup_date").alias("activity_month"),
    )
    .agg(
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_rows"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.round(F.sum("credit_limit_avg_3_prior") / F.nullif(F.count(F.when(F.col("segment") == "Consumer", 1)), F.lit(0)), 2).alias("credit_limit_avg_3_prior_per_consumer"),
    )
)
