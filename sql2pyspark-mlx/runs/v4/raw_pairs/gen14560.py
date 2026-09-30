from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("credit_limit").asc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())

ranked = (
    df_customers
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

valid = (
    ranked
    .filter((F.col("signup_date") > F.lit("2023-06-15").cast("date")) & F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "country",
        "signup_date",
        "credit_limit",
        (F.col("age") * F.col("latest_rank")).alias("age_times_latest_rank"),
        F.when(F.col("city") == "New York", 1).otherwise(0).alias("is_new_york"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.start_date").alias("subscription_start_date"))
)

result = (
    with_last
    .select("id", "name", "credit_limit", "is_new_york", "subscription_start_date")
    .orderBy(F.col("is_new_york").desc())
)
