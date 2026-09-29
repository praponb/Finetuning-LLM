from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("age", "id")
w2 = Window.orderBy(F.col("credit_limit").desc(), "id")

with_windows = (
    df_customers
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("credit_limit_bucket", F.ntile(5).over(w2))
)

result = (
    with_windows
    .withColumn("week_start", F.date_trunc("WEEK", "signup_date"))
    .withColumn("credit_limit_tier", F.when(F.col("credit_limit") >= 16200, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["week_start", "credit_limit_tier", "segment"], ["week_start", "credit_limit_tier"]],
        "week_start",
        "credit_limit_tier",
        "segment",
    )
    .agg(
        F.bit_and("age").alias("age_bit_and"),
        F.round(F.sum("credit_limit_bucket") / F.nullif(F.count(F.when(F.col("segment") == "Consumer", 1)), F.lit(0)), 2).alias("credit_limit_bucket_per_consumer"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("num_cities") < 1000)
    .orderBy("credit_limit_bucket_per_consumer")
)
