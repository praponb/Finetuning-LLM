from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("age", "id")
w_ordered = Window.orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("age", "id")
w4 = Window.partitionBy("city").orderBy("age_bucket", "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())
w_recent = Window.partitionBy("city").orderBy("prev_credit_limit", "id").rowsBetween(-6, Window.currentRow)

enriched = (
    df_customers
    .filter(F.col("email").like("%@startup.io"))
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.last("age", ignorenulls=True).over(w_ordered).alias("last_known_age"),
        F.ntile(10).over(w_grp).alias("age_bucket"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "event_seq",
        "last_known_age",
        "age_bucket",
        F.lead("age_bucket").over(w4).alias("next_age_bucket"),
        F.lag("age_bucket").over(w4).alias("prev_age_bucket"),
        F.lead("credit_limit").over(w4).alias("next_credit_limit"),
        F.lag("credit_limit").over(w4).alias("prev_credit_limit"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

with_latest = (
    metrics
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.start_date").alias("subscription_start_date"))
)

scored = (
    with_latest
    .withColumn("last7_avg_seq_no", F.avg("seq_no").over(w_recent))
    .withColumn("last7_max_seq_no", F.max("seq_no").over(w_recent))
    .withColumn("last7_sum_seq_no", F.sum("seq_no").over(w_recent))
)

result = (
    scored
    .groupBy("segment")
    .pivot("country", ["UK", "SG", "US"])
    .agg(
        F.sum("next_credit_limit").alias("amount"),
        F.count(F.lit(1)).alias("item_count"),
    )
    .select(
        "segment",
        F.col("UK_amount").alias("uk_amount"),
        F.col("UK_item_count").alias("uk_item_count"),
        F.col("SG_amount").alias("sg_amount"),
        F.col("SG_item_count").alias("sg_item_count"),
        F.col("US_amount").alias("us_amount"),
        F.col("US_item_count").alias("us_item_count"),
    )
)
