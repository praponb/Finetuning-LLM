from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc_nulls_first())
w2 = Window.partitionBy("segment").orderBy(F.col("signup_date").asc_nulls_first())
w3 = Window.partitionBy("country", "segment").orderBy("age", "id")
w4 = Window.partitionBy("segment", "country")
w5 = Window.orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("country").orderBy("first_known_age", "id").rowsBetween(-3, Window.currentRow)
w9 = Window.partitionBy("country")

ranked = (
    df_customers
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

ranked_customers = (
    ranked
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
    )
)

metrics = (
    ranked_customers
    .filter(F.col("age") < 47)
    .withColumn("prev_credit_limit", F.lag("credit_limit", 1).over(w3))
    .withColumn("segment_max_age", F.max("age").over(w4))
    .withColumn("cumulative_age", F.sum("age").over(w5))
    .withColumn("first_known_age", F.first("age", ignorenulls=True).over(w6))
)

metrics_customers = (
    metrics
    .select(
        "id",
        "name",
        "email",
        "country",
        "signup_date",
        "age",
        "prev_credit_limit",
        "cumulative_age",
        "first_known_age",
        F.sum("segment_max_age").over(w_grp).alias("running_sum_segment_max_age"),
        F.count("segment_max_age").over(w_grp).alias("running_count_segment_max_age"),
        F.avg("segment_max_age").over(w_grp).alias("running_avg_segment_max_age"),
    )
)

with_windows = (
    metrics_customers
    .select(
        "id",
        "email",
        "signup_date",
        "age",
        "prev_credit_limit",
        "running_avg_segment_max_age",
        F.min("running_sum_segment_max_age").over(w_hist).alias("last4_min_running_sum_segment_max_age"),
        F.max("running_sum_segment_max_age").over(w_hist).alias("last4_max_running_sum_segment_max_age"),
        F.avg("running_sum_segment_max_age").over(w_hist).alias("last4_avg_running_sum_segment_max_age"),
        F.round((F.col("age") / F.sum("age").over(w9)), 4).alias("age_share"),
    )
)

result = (
    with_windows
    .groupBy(
        F.date_trunc("week", "signup_date").alias("week_start"),
        F.when(F.col("age") >= 71, "HIGH").otherwise("LOW").alias("age_tier"),
    )
    .agg(
        F.avg("prev_credit_limit").alias("avg_prev_credit_limit"),
        F.bit_and("age").alias("age_bit_and"),
        F.round(F.avg("last4_max_running_sum_segment_max_age"), 2).alias("avg_last4_max_running_sum_segment_max_age"),
        F.avg("running_avg_segment_max_age").alias("avg_running_avg_segment_max_age"),
    )
)
