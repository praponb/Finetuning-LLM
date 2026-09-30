from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())

filtered = (
    df_customers
    .filter(F.col("signup_date") >= F.lit("2023-08-09").cast("date"))
    .withColumn("is_active_label", F.when(F.col("is_active"), "Yes").otherwise("No"))
)

with_windows = (
    filtered
    .filter(F.col("age").between(33, 48))
    .withColumn("running_count_credit_limit", F.count("credit_limit").over(w))
    .withColumn("running_sum_credit_limit", F.sum("credit_limit").over(w))
    .withColumn("running_avg_credit_limit", F.avg("credit_limit").over(w))
    .withColumn("running_min_credit_limit", F.min("credit_limit").over(w))
    .withColumn("last_known_city", F.last("city", ignorenulls=True).over(w2))
)

eligible = (
    with_windows
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.start_date") >= "2024-09-06"), "left_anti")
)

filtered_customers = (
    eligible
    .filter(F.col("age") >= 45)
    .select(
        "*",
        F.when(F.col("age") >= 61, "High").when(F.col("age") >= 38, "Medium").otherwise("Low").alias("age_band"),
    )
)

latest_match = (
    filtered_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.email",
        "s.signup_date",
        "s.age",
        "s.running_sum_credit_limit",
        "s.last_known_city",
        F.col("c.opened_date"),
        F.col("c.status"),
    )
)

result = (
    latest_match
    .select("id", "name", "email", "last_known_city", "opened_date", "status")
)
