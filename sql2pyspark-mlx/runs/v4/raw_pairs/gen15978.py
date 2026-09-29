from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

valid = (
    df_customers
    .filter(F.col("email").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        F.coalesce("age", F.lit(0)).alias("age_filled"),
        F.coalesce("city", F.lit("OTHER")).alias("resolved_city"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.customer_id").isin([9, 35])), "left_anti")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.discount"))
)

kept_keys = (
    with_latest
    .select("segment", "country")
    .intersectAll(
        df_customers
        .filter(F.year("signup_date") == 2024)
        .select("segment", "country")
    )
)

kept_rows = (
    with_latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("segment").isNotNull())
    .withColumn("is_active_label", F.when(F.col("is_active"), "Yes").otherwise("No"))
    .withColumn("resolved_segment", F.coalesce("segment", F.lit("OTHER")))
)

result = (
    cleaned
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("activity_month"), "city")
    .agg(
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_rows"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.count("*").alias("cnt"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("new_york_rows").desc())
)
