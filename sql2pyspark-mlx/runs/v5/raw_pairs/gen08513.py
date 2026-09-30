from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan")
w4 = Window.partitionBy("customer_id", "city").orderBy("first_known_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("first_known_monthly_fee").desc(), "id")
w6 = Window.orderBy(F.col("plan_max_monthly_fee").desc(), "id")

metrics = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 190)
    .select(
        "id",
        "customer_id",
        F.sum("monthly_fee").over(w).alias("cumulative_monthly_fee"),
        F.first("monthly_fee", ignorenulls=True).over(w2).alias("first_known_monthly_fee"),
        F.max("monthly_fee").over(w3).alias("plan_max_monthly_fee"),
    )
)

with_dims = (
    metrics
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.country"))
)

joined = (
    with_dims
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.name"), F.col("d.email"), F.col("d.city"))
)

cleaned = (
    joined
    .filter(F.col("plan_max_monthly_fee") > 97)
    .select(
        "id",
        "customer_id",
        "cumulative_monthly_fee",
        "first_known_monthly_fee",
        "plan_max_monthly_fee",
        "email",
        "city",
        F.greatest("plan_max_monthly_fee", "first_known_monthly_fee").alias("greatest_plan_max_monthly_fee_first_known_monthly_fee"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "cumulative_monthly_fee",
        "first_known_monthly_fee",
        "plan_max_monthly_fee",
        "email",
        "greatest_plan_max_monthly_fee_first_known_monthly_fee",
        "city_clean",
        F.first("plan_max_monthly_fee", ignorenulls=True).over(w4).alias("first_known_plan_max_monthly_fee"),
        F.ntile(4).over(w5).alias("first_known_monthly_fee_bucket"),
        F.ntile(10).over(w6).alias("plan_max_monthly_fee_bucket"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("plan_max_monthly_fee_bucket") >= 9, "PREMIUM").otherwise("STANDARD").alias("plan_max_monthly_fee_bucket_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.avg("cumulative_monthly_fee").alias("avg_cumulative_monthly_fee"),
    )
)
