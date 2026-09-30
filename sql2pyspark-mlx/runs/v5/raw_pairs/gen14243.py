from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("signup_date").asc_nulls_first(), "id")
w2 = Window.partitionBy("city").orderBy(F.col("credit_limit").desc(), "id")
w3 = Window.partitionBy("city", "segment").orderBy("age", "id")
w4 = Window.partitionBy("segment").orderBy(F.col("greatest_age_credit_limit").desc(), "id")

valid = (
    df_customers
    .filter(F.col("signup_date") >= F.lit("2023-03-18").cast("date"))
    .withColumn("clean_name", F.trim("name"))
    .withColumn("greatest_age_credit_limit", F.greatest("age", "credit_limit"))
)

deduped = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        "clean_name",
        "greatest_age_credit_limit",
    )
)

latest = (
    deduped
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    latest
    .select(
        "id",
        "city",
        "credit_limit",
        "age",
        F.lead("greatest_age_credit_limit").over(w3).alias("next_greatest_age_credit_limit"),
        F.lead("credit_limit").over(w3).alias("next_credit_limit"),
        F.lag("credit_limit").over(w3).alias("prev_credit_limit"),
        F.row_number().over(w3).alias("event_seq"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

result = (
    metrics
    .groupBy("seq_no")
    .agg(
        F.bit_or("age").alias("age_bit_or"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_rows"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.percentile_approx("next_greatest_age_credit_limit", 0.5).alias("next_greatest_age_credit_limit_p50"),
    )
    .orderBy("bangkok_rows")
)
