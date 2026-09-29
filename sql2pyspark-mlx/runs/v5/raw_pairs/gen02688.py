from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("signup_date").desc_nulls_first(), "id")
w2 = Window.orderBy("age", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("country", "segment").orderBy(F.col("age").desc(), "id")
w4 = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "age",
        F.trim("city").alias("clean_city"),
    )
)

latest = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "age",
        "clean_city",
    )
)

filtered_customers = (
    latest
    .filter((F.col("signup_date") >= F.lit("2023-03-10").cast("date")) & (~F.col("city").eqNullSafe(F.lit("Chiang Mai"))))
    .select(
        "*",
        F.round((F.col("age") * 100), 2).alias("adjusted_age"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled"),
    )
)

with_windows = (
    filtered_customers
    .filter(F.col("signup_date") >= "2024-02-24")
    .select(
        "id",
        "email",
        "country",
        "signup_date",
        "age",
        "clean_city",
        "city_filled",
        F.avg("adjusted_age").over(w2).alias("adjusted_age_avg_2_prior"),
        F.row_number().over(w3).alias("seq_no"),
        F.first("age", ignorenulls=True).over(w4).alias("first_known_age"),
    )
)

result = (
    with_windows
    .withColumn("age_tier", F.when(F.col("age") >= 63, "PREMIUM").otherwise("STANDARD"))
    .groupingSets([["age_tier", "seq_no"], ["age_tier"], []], "age_tier", "seq_no")
    .agg(
        F.round(F.sum("seq_no") / F.nullif(F.count(F.when(F.col("country") == "SG", 1)), F.lit(0)), 2).alias("seq_no_per_sg"),
        F.stddev_samp("age").alias("age_stddev"),
        F.count("*").alias("event_count"),
        F.grouping_id().alias("grouping_id"),
    )
)
