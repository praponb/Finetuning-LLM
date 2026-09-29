from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("signup_date").desc(), "id")
w_hist = Window.partitionBy("segment", "country").orderBy(F.col("age").desc(), "id")
w3 = Window.partitionBy("segment").orderBy("row_num", "id").rowsBetween(-6, -1)
w4 = Window.orderBy("age", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("segment").orderBy("row_num", "id")
w6 = Window.partitionBy("segment", "country").orderBy(F.col("row_num").desc(), "id")
w7 = Window.partitionBy("segment", "country").orderBy(F.col("greatest_is_corporate_avg_6_age_avg_6_prior").asc_nulls_first(), "id")

cleaned = (
    df_customers
    .filter(F.col("country").isin(["SG", "UK"]))
    .select(
        "id",
        "email",
        "country",
        "segment",
        "signup_date",
        "age",
        F.upper("segment").alias("segment_upper"),
        F.lower("segment").alias("segment_lower"),
    )
)

latest = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

valid = (
    latest
    .filter((F.col("signup_date") > F.lit("2023-11-04").cast("date")) & (F.col("row_num") <= 1))
    .withColumn("is_corporate", F.when(F.col("segment") == "Corporate", 1).otherwise(0))
    .withColumn("segment_title", F.initcap(F.trim("segment")))
    .withColumn("resolved_segment", F.coalesce("segment", F.lit("UNKNOWN")))
)

enriched = (
    valid
    .select(
        "*",
        F.ntile(10).over(w_hist).alias("age_bucket"),
        F.avg("age").over(w3).alias("age_avg_6_prior"),
        F.avg("is_corporate").over(w4).alias("is_corporate_avg_6"),
        F.lag("is_corporate", 2).over(w5).alias("prev_is_corporate"),
        F.row_number().over(w6).alias("seq_no"),
    )
)

cleaned_customers = (
    enriched
    .filter(F.col("signup_date") > "2023-03-01")
    .select(
        "id",
        "email",
        "country",
        "segment",
        "age",
        "segment_lower",
        "is_corporate",
        "segment_title",
        "resolved_segment",
        "age_avg_6_prior",
        "is_corporate_avg_6",
        "prev_is_corporate",
        F.greatest("is_corporate_avg_6", "age_avg_6_prior").alias("greatest_is_corporate_avg_6_age_avg_6_prior"),
        F.lower("country").alias("country_lower"),
        F.coalesce("segment", F.lit("DIRECT")).alias("segment_filled"),
    )
)

valid_customers = (
    cleaned_customers
    .filter(F.col("segment").isNotNull())
    .select(
        "id",
        "country",
        "segment",
        "age",
        "is_corporate",
        "segment_title",
        "resolved_segment",
        "is_corporate_avg_6",
        "prev_is_corporate",
        "greatest_is_corporate_avg_6_age_avg_6_prior",
        "country_lower",
        "segment_filled",
        F.greatest("prev_is_corporate", "age_avg_6_prior").alias("greatest_prev_is_corporate_age_avg_6_prior"),
    )
)

latest_customers = (
    valid_customers
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
)

result = (
    latest_customers
    .select(
        "id",
        "country",
        "segment",
        "is_corporate",
        "resolved_segment",
        "is_corporate_avg_6",
        "prev_is_corporate",
        "greatest_is_corporate_avg_6_age_avg_6_prior",
        "segment_filled",
        F.when(F.col("segment").rlike("^(Consumer|SMB)$"), "OTHER_ZONE").otherwise("CORE").alias("segment_zone"),
    )
)
