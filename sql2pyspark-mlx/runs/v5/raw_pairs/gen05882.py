from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("credit_limit", "id")
w2 = Window.orderBy("signup_date", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("country").orderBy(F.col("prev_credit_limit").desc_nulls_first(), "id")
w4 = Window.partitionBy("city").orderBy(F.col("age").desc())

valid = (
    df_customers
    .filter(F.col("city").isNotNull() & (~F.col("city").eqNullSafe(F.lit("Singapore"))))
    .withColumn("country_group", F.when(F.col("country").isin(["TH", "US"]), "Core").otherwise("Other"))
    .withColumn("segment_clean", F.coalesce("segment", F.lit("DIRECT")))
)

cleaned = (
    valid
    .filter((F.col("signup_date") > F.lit("2023-06-18").cast("date")) & (F.col("segment").eqNullSafe(F.lit("Corporate"))))
    .withColumn("is_active_flag", F.col("is_active").cast("int"))
    .withColumn("clean_country", F.trim("country"))
)

with_windows = (
    cleaned
    .withColumn("prev_is_active_flag", F.lag("is_active_flag").over(w))
    .withColumn("prev_credit_limit", F.lag("credit_limit").over(w))
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("credit_limit_avg_3", F.avg("credit_limit").over(w2))
)

latest = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

deduped = (
    latest
    .withColumn("dedup_rank_2", F.rank().over(w4))
    .filter(F.col("dedup_rank_2") == 1)
    .select(
        "id",
        "city",
        "segment",
        "signup_date",
        "country_group",
        "clean_country",
        "prev_credit_limit",
        "dedup_rank",
    )
)

result = (
    deduped
    .groupBy("city", "segment")
    .agg(
        F.round(F.sum("prev_credit_limit") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("prev_credit_limit_per_singapore"),
        F.max("prev_credit_limit").alias("max_prev_credit_limit"),
    )
    .orderBy(F.col("prev_credit_limit_per_singapore").asc_nulls_last())
)
