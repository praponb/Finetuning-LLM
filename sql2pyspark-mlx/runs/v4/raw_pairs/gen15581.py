from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("start_date_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("start_date_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.partitionBy("status", "plan").orderBy(F.col("monthly_fee").desc(), "id")
w5 = Window.partitionBy("status").orderBy("start_date", "id")
w6 = Window.partitionBy("customer_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("status", "customer_id").orderBy("start_date", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.partitionBy("customer_id")

filtered = (
    df_subscriptions
    .filter(F.col("plan") == "premium")
    .withColumn("start_date_period", F.date_trunc("MONTH", "start_date"))
)

scored = (
    filtered
    .withColumn("first_known_monthly_fee", F.first("monthly_fee", ignorenulls=True).over(w))
    .withColumn("monthly_fee_avg_90d", F.avg("monthly_fee").over(w2))
    .withColumn("monthly_fee_avg_30d", F.avg("monthly_fee").over(w3))
    .withColumn("monthly_fee_bucket", F.ntile(10).over(w4))
    .withColumn("prev_monthly_fee", F.lag("monthly_fee", 2).over(w5))
)

enriched = (
    scored
    .select(
        "*",
        F.last("monthly_fee_avg_30d", ignorenulls=True).over(w6).alias("last_known_monthly_fee_avg_30d"),
        F.avg("first_known_monthly_fee").over(w_recent).alias("first_known_monthly_fee_avg_2"),
        F.max("monthly_fee_avg_30d").over(w8).alias("customer_id_max_monthly_fee_avg_30d"),
    )
)

kept_keys = (
    enriched
    .select("status", "customer_id")
    .intersectAll(
        df_subscriptions
        .filter(F.col("start_date").between("2023-12-01", "2024-04-01"))
        .select("status", "customer_id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

enriched_subscriptions = (
    kept_rows
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.age"), F.col("p.city"))
)

joined = (
    enriched_subscriptions
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.age").alias("customer_age"), F.col("d.country"))
)

with_dims = (
    joined
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.email"))
)

result = (
    with_dims
    .groupBy(F.date_trunc("month", "start_date").alias("activity_month"))
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.count_if(F.col("plan") == "standard").alias("standard_events"),
    )
)
