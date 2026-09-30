from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country")
w4 = Window.orderBy("credit_limit", "id")
w5 = Window.orderBy("age_share", "id")
w6 = Window.partitionBy("country").orderBy("last_known_age", "id").rowsBetween(-5, -1)
w7 = Window.partitionBy("country").orderBy("last_known_age", "id").rowsBetween(-2, -1)
w8 = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(-2, Window.currentRow)

cleaned = (
    df_customers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.when(F.col("credit_limit") >= 14300, "High").when(F.col("credit_limit") >= 8200, "Medium").otherwise("Low").alias("credit_limit_band"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.order_date") <= F.col("s.signup_date")), "left_anti")
)

base = (
    not_flagged
    .filter(F.col("city").isNotNull() & (F.col("signup_date") > F.lit("2023-05-23").cast("date")))
    .select(
        "id",
        "name",
        "email",
        "country",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.lower("email").alias("email_lower"),
        F.length("segment").alias("segment_length"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean"),
    )
)

with_windows = (
    base
    .filter(~F.col("is_active"))
    .withColumn("cumulative_age", F.sum("age").over(w))
    .withColumn("last_known_age", F.last("age", ignorenulls=True).over(w2))
    .withColumn("corr_credit_limit_age", F.corr("credit_limit", "age").over(w3))
    .withColumn("credit_limit_bucket", F.ntile(5).over(w4))
    .withColumn("age_share", F.round((F.col("age") / F.sum("age").over(w3)), 4))
)

valid = (
    with_windows
    .filter(F.col("signup_date") > F.lit("2023-08-18").cast("date"))
    .withColumn("age_filled", F.coalesce("age", F.lit(0)))
)

base_customers = (
    valid
    .filter(F.col("signup_date") >= F.lit("2023-09-15").cast("date"))
    .select("*", F.col("is_active").cast("int").alias("is_active_flag"))
)

scored = (
    base_customers
    .select(
        "*",
        F.ntile(4).over(w5).alias("age_share_bucket"),
        F.max("age_filled").over(w3).alias("country_max_age_filled"),
        F.avg("last_known_age").over(w6).alias("last_known_age_avg_5_prior"),
        F.avg("cumulative_age").over(w7).alias("cumulative_age_avg_2_prior"),
        F.avg("corr_credit_limit_age").over(w8).alias("corr_credit_limit_age_avg_2"),
    )
)

result = (
    scored
    .select(
        "id",
        "name",
        "signup_date",
        "credit_limit",
        "age",
        "email_lower",
        "segment_length",
        "cumulative_age",
        "credit_limit_bucket",
    )
    .orderBy(F.col("signup_date").asc_nulls_first(), "credit_limit")
)
