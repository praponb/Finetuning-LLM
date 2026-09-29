from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "segment")
w2 = Window.partitionBy("country", "segment")
w3 = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("segment", "city").orderBy(F.col("signup_date").desc(), "id")
w5 = Window.orderBy("country_count_age_filled", "id")
w6 = Window.orderBy(F.col("age_share").desc(), "id")
w7 = Window.orderBy("cumulative_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_customers
    .filter(F.col("signup_date") > F.to_date(F.lit("2023-09-07")))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        F.coalesce("age", F.lit(0)).alias("age_filled"),
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.round((F.col("age") / F.sum("age").over(w)), 4).alias("age_share"),
        F.count("age_filled").over(w2).alias("country_count_age_filled"),
        F.sum("age").over(w3).alias("cumulative_age"),
        F.row_number().over(w_recent).alias("event_seq"),
    )
)

with_windows_customers = (
    with_windows
    .withColumn("seq_no", F.row_number().over(w5))
    .withColumn("age_share_bucket", F.ntile(4).over(w6))
    .withColumn("running_age", F.sum("age").over(w7))
)

result = (
    with_windows_customers
    .groupBy("seq_no")
    .agg(
        F.count("*").alias("row_count"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.max(F.when(F.col("city") == "Tokyo", F.col("cumulative_age"))).alias("tokyo_max_cumulative_age"),
        F.percentile_approx("running_age", [0.25, 0.5, 0.75]).alias("running_age_quartiles"),
        F.bit_and("age").alias("age_bit_and"),
    )
    .orderBy(F.col("age_bit_and").asc_nulls_first())
)
