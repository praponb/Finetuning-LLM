from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())

valid = (
    df_customers
    .filter((~F.col("city").eqNullSafe(F.lit("Bangkok"))) & (F.col("signup_date") > F.to_date(F.lit("2023-02-11"))))
    .select(
        "*",
        F.when(F.col("segment") == "Consumer", 1).otherwise(0).alias("is_consumer"),
    )
)

base = (
    valid
    .filter((F.col("signup_date") >= F.lit("2023-06-15").cast("date")) & F.col("segment").isNotNull())
    .select(
        "*",
        F.col("is_active").cast("int").alias("is_active_flag"),
        (F.col("is_consumer") + F.col("age")).alias("is_consumer_plus_age"),
    )
)

filtered = (
    base
    .filter(F.col("signup_date") >= F.to_date(F.lit("2023-02-19")))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "is_consumer",
        "is_consumer_plus_age",
        F.round("credit_limit", 0).alias("credit_limit_rounded"),
    )
)

valid_customers = (
    filtered
    .filter(F.col("signup_date") > F.lit("2023-03-04").cast("date"))
    .withColumn("is_consumer_plus_age_int", F.col("is_consumer_plus_age").cast("int"))
)

latest_match = (
    valid_customers
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status"))
)

result = (
    latest_match
    .select(
        "id",
        "segment",
        F.col("credit_limit").alias("CREDIT_LIMIT_VAL"),
        F.col("credit_limit_rounded").alias("CREDIT_LIMIT_ROUNDED"),
    )
    .unpivot(
        ids=["id", "segment"],
        values=["CREDIT_LIMIT_VAL", "CREDIT_LIMIT_ROUNDED"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
