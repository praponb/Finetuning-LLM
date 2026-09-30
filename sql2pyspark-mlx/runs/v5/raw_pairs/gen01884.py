from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("end_date").asc_nulls_last(), "id")

scored = (
    df_subscriptions
    .filter(F.col("monthly_fee").isNotNull())
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4))
    .withColumn("first_known_monthly_fee", F.first("monthly_fee", ignorenulls=True).over(w2))
    .withColumn("running_monthly_fee", F.sum("monthly_fee").over(w2))
)

joined = (
    scored
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.city"), F.col("d.name"))
)

latest = (
    joined
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

with_dims = (
    latest
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select(
        "src.*",
        F.col("p.credit_limit"),
        F.col("p.email"),
        F.col("p.city").alias("customer_city"),
    )
)

filtered = (
    with_dims
    .filter((F.col("end_date") > F.lit("2023-11-02").cast("date")) & F.col("city").isNotNull())
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "status",
        "monthly_fee_share",
        "city",
        "rn",
        "credit_limit",
        "email",
        F.date_format("start_date", "yyyy-MM-dd").alias("start_date_label"),
    )
)

stats = (
    filtered
    .groupBy(F.date_trunc("week", "start_date").alias("week_start"))
    .agg(
        F.round(F.avg("monthly_fee_share"), 2).alias("avg_monthly_fee_share"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.avg(F.when(F.col("city") == "Singapore", F.col("credit_limit"))).alias("singapore_avg_credit_limit"),
        F.percentile_approx("credit_limit", 0.5).alias("credit_limit_p50"),
        F.percentile_approx("monthly_fee", 0.9).alias("monthly_fee_p90"),
    )
)

result = (
    stats
    .select(
        "week_start",
        "avg_monthly_fee_share",
        "chiang_mai_count",
        "singapore_avg_credit_limit",
        "credit_limit_p50",
        "monthly_fee_p90",
    )
    .orderBy(F.col("monthly_fee_p90").desc_nulls_last())
)
