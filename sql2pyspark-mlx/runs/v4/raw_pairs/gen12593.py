from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-10-18").cast("date")) & (F.col("age") > 31))
    .withColumn("age_rounded", F.round("age", 2))
    .withColumn("city_filled", F.coalesce("city", F.lit("DIRECT")))
)

with_latest = (
    filtered
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.discount").alias("order_discount"))
)

with_last = (
    with_latest
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.country").alias("session_country"),
        F.col("c.started_at").alias("session_started_at"),
    )
)

eligible = (
    with_last
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.order_date") >= F.col("s.signup_date")), "left_semi")
)

result = (
    eligible
    .select(
        "id",
        "country",
        F.col("credit_limit").alias("CREDIT_LIMIT_VAL"),
        F.col("order_discount").alias("ORDER_DISCOUNT"),
        F.col("age_rounded").alias("AGE_ROUNDED_VAL"),
    )
    .unpivot(
        ["id", "country"],
        ["CREDIT_LIMIT_VAL", "ORDER_DISCOUNT", "AGE_ROUNDED_VAL"],
        "metric",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)
