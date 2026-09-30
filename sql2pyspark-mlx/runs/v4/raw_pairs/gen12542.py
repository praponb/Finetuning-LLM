from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("age")
w2 = Window.partitionBy("country", "city").orderBy("signup_date", "id")
w3 = Window.orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country")
w5 = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc(), "id")
w6 = Window.partitionBy("country", "city").orderBy("credit_limit_share", "id")
w_ordered = Window.partitionBy("country").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("city", "country").orderBy(F.col("event_seq").asc_nulls_last())
w_ordered_2 = Window.partitionBy("country").orderBy("cumulative_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_customers
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

deduped = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    deduped
    .withColumn("cumulative_credit_limit", F.sum("credit_limit").over(w3))
    .withColumn("credit_limit_share", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w4)), 4))
    .withColumn("event_seq", F.row_number().over(w5))
)

scored = (
    metrics
    .withColumn("credit_limit_share_bucket", F.ntile(4).over(w6))
    .withColumn("last_known_event_seq", F.last("event_seq", ignorenulls=True).over(w_ordered))
    .withColumn("event_seq_rank", F.rank().over(w8))
    .withColumn("last_known_event_seq_2", F.last("event_seq", ignorenulls=True).over(w_ordered_2))
)

result = (
    scored
    .groupBy(
        "country",
        F.date_trunc("YEAR", "signup_date").alias("year_start"),
        F.when(F.col("last_known_event_seq_2") >= 9, "HIGH").otherwise("LOW").alias("last_known_event_seq_2_tier"),
    )
    .agg(
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
        F.percentile_approx("event_seq_rank", 0.5).alias("event_seq_rank_p50"),
    )
)
