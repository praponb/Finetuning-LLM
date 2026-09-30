from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy(F.col("age").desc_nulls_last())
w_recent = Window.partitionBy("country", "segment").orderBy("age", "id")
w4 = Window.partitionBy("city", "segment").orderBy(F.col("age_bucket").desc(), "id")
w5 = Window.partitionBy("segment")
w6 = Window.partitionBy("city").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("city")
w8 = Window.partitionBy("segment").orderBy("age", "id")
w9 = Window.partitionBy("city").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("segment_min_age_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_last = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.end_date"))
)

with_windows = (
    with_last
    .filter(F.col("end_date") < "2025-08-01")
    .withColumn("age_cume_dist", F.cume_dist().over(w2))
    .withColumn("age_bucket", F.ntile(4).over(w_recent))
)

ranked = (
    with_windows
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

scored = (
    ranked
    .select(
        "id",
        "name",
        "email",
        "signup_date",
        "is_active",
        "credit_limit",
        "end_date",
        "age_cume_dist",
        "age_bucket",
        "row_num",
        F.min("age_bucket").over(w5).alias("segment_min_age_bucket"),
        F.sum("credit_limit").over(w6).alias("cumulative_credit_limit"),
        F.avg("age_cume_dist").over(w7).alias("city_avg_age_cume_dist"),
        F.row_number().over(w8).alias("event_seq"),
        F.first("age_bucket", ignorenulls=True).over(w9).alias("first_known_age_bucket"),
    )
)

enriched = (
    scored
    .withColumn("running_avg_cumulative_credit_limit", F.avg("cumulative_credit_limit").over(w10))
    .withColumn("running_sum_cumulative_credit_limit", F.sum("cumulative_credit_limit").over(w10))
    .withColumn("running_max_cumulative_credit_limit", F.max("cumulative_credit_limit").over(w10))
    .withColumn("running_count_cumulative_credit_limit", F.count("cumulative_credit_limit").over(w10))
)

result = (
    enriched
    .select(
        "id",
        "name",
        "email",
        "signup_date",
        "is_active",
        "credit_limit",
        "end_date",
        "age_cume_dist",
        "age_bucket",
        F.when(F.col("name").rlike("^(Henry Ford|Mia Rossi)$"), "PRIORITY").when(F.col("name").rlike("^(Alice Wong|bob smith)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("name_zone"),
    )
    .orderBy("signup_date", "name_zone")
)
