from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("segment", "city").orderBy("credit_limit", "id")
w2 = Window.partitionBy("city", "segment").orderBy("next_credit_limit", "id")
w3 = Window.partitionBy("city", "segment").orderBy("row_seq", "id")
w4 = Window.partitionBy("segment").orderBy(F.col("prev_next_credit_limit").desc(), "id")

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-05-14").cast("date")) & F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        F.upper("email").alias("email_upper"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

with_windows = (
    filtered
    .filter(F.col("segment").isin(["SMB", "Consumer", "Corporate"]))
    .withColumn("next_credit_limit", F.lead("credit_limit").over(w_grp))
    .withColumn("row_seq", F.row_number().over(w_grp))
)

metrics = (
    with_windows
    .withColumn("next_row_seq", F.lead("row_seq").over(w2))
    .withColumn("prev_row_seq", F.lag("row_seq").over(w2))
    .withColumn("prev_next_credit_limit", F.lag("next_credit_limit").over(w2))
    .withColumn("next_next_credit_limit", F.lead("next_credit_limit").over(w2))
    .withColumn("row_seq_2", F.row_number().over(w2))
    .withColumn("prev_credit_limit", F.lag("credit_limit", 2).over(w3))
)

base = (
    metrics
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-05-08"))) & (F.col("next_row_seq") < 6))
    .select(
        "id",
        "email",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "email_upper",
        "next_row_seq",
        "prev_row_seq",
        "prev_next_credit_limit",
        "prev_credit_limit",
        F.when(F.col("prev_credit_limit") >= 13400, "High").when(F.col("prev_credit_limit") >= 4900, "Medium").otherwise("Low").alias("prev_credit_limit_level"),
        F.round((F.col("prev_credit_limit") / F.col("prev_row_seq")), 2).alias("prev_credit_limit_per_prev_row_seq"),
        F.coalesce("segment", F.lit("UNKNOWN")).alias("segment_clean"),
    )
)

latest = (
    base
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .groupBy(
        F.when(F.col("prev_credit_limit_per_prev_row_seq") >= 60, "LARGE").otherwise("SMALL").alias("prev_credit_limit_per_prev_row_seq_tier"),
    )
    .agg(
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.avg(F.when(F.col("city") == "London", F.col("credit_limit"))).alias("london_avg_credit_limit"),
        F.max("credit_limit").alias("max_credit_limit"),
        F.round(F.sum("prev_credit_limit") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("prev_credit_limit_per_london"),
        F.round(F.avg("prev_credit_limit"), 2).alias("avg_prev_credit_limit"),
    )
)
