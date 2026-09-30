from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("age_minus_credit_limit").asc_nulls_first(), "id")
w2 = Window.partitionBy("activity_month").orderBy(F.col("smb_age_minus_credit_limit").desc())

valid = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-09-22").cast("date")) & F.col("segment").isNotNull())
    .select("*", F.concat("country", F.lit(", "), "name").alias("country_name"))
)

cleaned = (
    valid
    .filter(F.col("city").isin(["Tokyo", "Bangkok"]))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        (F.col("age") - F.col("credit_limit")).alias("age_minus_credit_limit"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") >= F.col("s.signup_date")), "left_semi")
)

deduped = (
    eligible
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "signup_date",
        "age_minus_credit_limit",
    )
)

result = (
    deduped
    .groupBy(
        F.date_trunc("MONTH", "signup_date").alias("activity_month"),
        F.date_trunc("QUARTER", "signup_date").alias("quarter_start"),
    )
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.sum(F.when(F.col("country") == "JP", F.col("age_minus_credit_limit"))).alias("jp_total_age_minus_credit_limit"),
        F.sum(F.when(F.col("segment") == "SMB", F.col("age_minus_credit_limit")).otherwise(0)).alias("smb_age_minus_credit_limit"),
    )
    .withColumn("smb_age_minus_credit_limit_percent_rank", F.percent_rank().over(w2))
)
