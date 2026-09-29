from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("end_date").desc(), "id")

filtered = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 130)
    .select(
        "*",
        F.when(F.col("plan") == "enterprise", 1).otherwise(0).alias("is_enterprise"),
    )
)

joined = (
    filtered
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.end_date",
        "e.status",
        F.col("c.country"),
        F.col("c.segment"),
    )
)

with_windows = joined.select("*", F.row_number().over(w).alias("row_seq"))

with_dims = (
    with_windows
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select(
        "src.*",
        F.col("c.country").alias("customer_country"),
        F.col("c.signup_date"),
    )
)

valid = (
    with_dims
    .filter(F.year("end_date") >= 2024)
    .select(
        "id",
        "customer_id",
        "end_date",
        "status",
        "segment",
        "row_seq",
        F.coalesce("segment", F.lit("N/A")).alias("segment_filled"),
        F.date_format("signup_date", "MMM yyyy").alias("signup_date_label"),
        F.coalesce("segment", F.lit("UNKNOWN")).alias("segment_clean"),
    )
)

base = (
    valid
    .filter((F.col("end_date") > F.to_date(F.lit("2023-11-15"))) & F.col("segment").isNotNull())
    .withColumn("status_prefix", F.substring("status", 1, 3))
)

result = (
    base
    .select(
        "id",
        "end_date",
        "status",
        "row_seq",
        "segment_filled",
        "signup_date_label",
        "segment_clean",
        "status_prefix",
        F.when(F.col("status").rlike("^(EXPIRED|PAUSED)$"), "CORE").when(F.col("status").rlike("^(ACTIVE|CANCELLED)$"), "GROUP_A").otherwise("PRIORITY").alias("status_zone"),
    )
)
