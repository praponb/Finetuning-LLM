from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").desc_nulls_first(), "id")
w2 = Window.partitionBy("city", "country").orderBy(F.col("age").desc_nulls_last(), "id")
w3 = Window.partitionBy("country").orderBy(F.col("age").asc_nulls_first(), "id")

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-05-24").cast("date")) & F.col("city").isNull())
    .select("*", F.concat_ws("/", "segment", "email").alias("segment_email_key"))
)

latest = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

deduped = (
    latest
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

valid = (
    deduped
    .filter(F.col("segment").isNotNull() & (F.col("credit_limit") < 14400))
    .select(
        "*",
        (F.col("age") + F.col("credit_limit")).alias("age_plus_credit_limit"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_filled"),
    )
)

deduped_customers = (
    valid
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

grouped = (
    deduped_customers
    .groupBy(F.when(F.col("age") >= 72, "HIGH").otherwise("LOW").alias("age_tier"))
    .agg(
        F.percentile_approx("age", 0.95).alias("age_p95"),
        F.max(F.when(F.col("country") == "US", F.col("age"))).alias("us_max_age"),
    )
)

result = grouped.select("age_tier", "us_max_age")
