from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment").orderBy(F.col("credit_limit").desc_nulls_last(), "id")
w2 = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")
w4 = Window.partitionBy("country").orderBy(F.col("signup_date").desc(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.start_date").desc(), F.col("c.id").desc())

latest = (
    df_customers
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    latest
    .filter(F.col("country") == "DE")
    .select(
        "*",
        F.last("age", ignorenulls=True).over(w2).alias("last_known_age"),
        F.ntile(3).over(w3).alias("age_bucket"),
    )
)

ranked = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

with_last = (
    ranked
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.start_date") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.monthly_fee"))
)

result = (
    with_last
    .select(
        "id",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "rank_in_group",
    )
)
