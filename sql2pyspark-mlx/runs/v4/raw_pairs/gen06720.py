from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country", "city").orderBy(F.col("signup_date").desc_nulls_last())
w3 = Window.partitionBy("country", "order_status").orderBy("signup_date", "id")
w4 = Window.partitionBy("city", "order_status").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_status").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-10-23"))) & F.col("city").isNotNull() & (F.length("email") < 9))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "signup_date",
        "is_active",
        F.when(F.col("country") == "TH", 1).otherwise(0).alias("is_th"),
    )
)

cleaned_customers = (
    cleaned
    .filter(F.col("signup_date") >= F.to_date(F.lit("2024-01-09")))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "signup_date",
        "is_active",
        F.trim("name").alias("clean_name"),
        F.regexp_extract("email", "@(.+)$", 1).alias("email_domain"),
    )
)

latest_match = (
    cleaned_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("order_status"))
)

deduped = (
    latest_match
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

metrics = (
    deduped
    .select(
        "*",
        F.row_number().over(w3).alias("seq_no"),
        F.first("order_status", ignorenulls=True).over(w4).alias("first_known_order_status"),
        F.first("city", ignorenulls=True).over(w5).alias("first_known_city"),
        F.last("order_status", ignorenulls=True).over(w6).alias("last_known_order_status"),
    )
)

result = (
    metrics
    .withColumn("activity_month", F.date_trunc("MONTH", "signup_date"))
    .withColumn("seq_no_tier", F.when(F.col("seq_no") >= 8, "PREMIUM").otherwise("STANDARD"))
    .rollup("activity_month", "seq_no_tier")
    .agg(
        F.sum("seq_no").alias("total_seq_no"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.percentile_approx("seq_no", 0.5).alias("seq_no_p50"),
        F.grouping("activity_month").alias("grouping_level"),
    )
    .filter(F.col("seq_no_p50") >= 0)
)
