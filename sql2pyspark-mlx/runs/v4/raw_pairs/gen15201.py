from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())

valid = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-11-13").cast("date")) & F.col("segment").isNotNull() & ~F.col("is_active"))
    .withColumn("country_title", F.initcap(F.trim("country")))
    .withColumn("credit_limit_per_age", F.round((F.col("credit_limit") / F.col("age")), 2))
    .withColumn("city_filled", F.coalesce("city", F.lit("N/A")))
)

with_last = (
    valid
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.balance"),
        F.col("c.opened_date").alias("account_opened_date"),
    )
)

cleaned = (
    with_last
    .filter((F.col("signup_date") >= F.lit("2023-07-05").cast("date")) & F.col("city").isNotNull() & ~F.col("email").like("%@gmail.com"))
    .select(
        "*",
        F.lower("country").alias("country_lower"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "city",
        "credit_limit_per_age",
        F.col("credit_limit").alias("CREDIT_LIMIT_VAL"),
        F.col("balance").alias("BALANCE_VAL"),
    )
    .unpivot(
        ["id", "city"],
        ["credit_limit_per_age", "CREDIT_LIMIT_VAL", "BALANCE_VAL"],
        "metric_name",
        "metric_value",
    )
)
