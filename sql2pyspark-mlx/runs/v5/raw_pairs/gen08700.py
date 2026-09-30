from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("segment").orderBy("credit_limit", "id")
w4 = Window.orderBy("account_opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("segment").orderBy("account_opened_date", "id")

cleaned = (
    df_customers
    .filter((F.col("country") == "JP") & (F.col("signup_date") >= F.lit("2023-05-09").cast("date")))
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
        F.regexp_replace("email", " ", "-").alias("email_slug"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.end_date"), F.col("c.start_date"))
)

with_last_customers = (
    with_last
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.email",
        "s.segment",
        "s.signup_date",
        "s.credit_limit",
        "s.email_slug",
        F.col("c.account_type"),
        F.col("c.opened_date").alias("account_opened_date"),
    )
)

enriched = (
    with_last_customers
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("cumulative_credit_limit", F.sum("credit_limit").over(w4))
    .withColumn("prev_credit_limit", F.lag("credit_limit", 1).over(w5))
)

result = (
    enriched
    .groupBy("account_type")
    .pivot("segment", ["Consumer", "Corporate"])
    .agg(F.max("row_seq").alias("value_sum"), F.count(F.lit(1)).alias("n"))
)
