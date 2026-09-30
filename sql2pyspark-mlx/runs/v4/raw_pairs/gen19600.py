from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("country", "segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("city").orderBy(F.col("cumulative_credit_limit").desc(), "id")
w5 = Window.orderBy("signup_date", "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())

metrics = (
    df_customers
    .withColumn("credit_limit_share", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w)), 4))
    .withColumn("cumulative_credit_limit", F.sum("credit_limit").over(w2))
    .withColumn("age_share", F.round((F.col("age") / F.sum("age").over(w)), 4))
)

with_latest = (
    metrics
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.monthly_fee"))
)

enriched = (
    with_latest
    .select(
        "*",
        F.min("age_share").over(w).alias("country_min_age_share"),
        F.row_number().over(w4).alias("row_seq"),
        F.lead("age_share", 1).over(w5).alias("next_age_share"),
    )
)

with_latest_customers = (
    enriched
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("order_status"))
)

result = (
    with_latest_customers
    .rollup(
        F.date_trunc("YEAR", "signup_date").alias("year_start"),
        F.date_trunc("MONTH", "signup_date").alias("activity_month"),
    )
    .agg(
        F.round(F.avg("age_share"), 2).alias("avg_age_share"),
        F.percentile_approx("credit_limit_share", 0.95).alias("credit_limit_share_p95"),
        F.percentile_approx("credit_limit_share", [0.25, 0.5, 0.75]).alias("credit_limit_share_quartiles"),
    )
    .filter(F.col("avg_age_share") > 0)
)
