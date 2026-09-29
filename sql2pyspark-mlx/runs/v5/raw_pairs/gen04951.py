from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "plan")
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc_nulls_last())

base = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2023-05-03").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        F.substring("status", 1, 4).alias("status_prefix"),
    )
)

remaining_keys = (
    base
    .select("plan", "customer_id")
    .intersect(
        df_subscriptions
        .filter(F.col("start_date").between("2023-11-01", "2024-02-01"))
        .select("plan", "customer_id")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .filter(F.col("plan").isin(["basic", "standard"]))
    .withColumn("customer_id_avg_monthly_fee", F.avg("monthly_fee").over(w))
)

scored_subscriptions = (
    scored
    .select(
        "*",
        F.round((F.col("customer_id_avg_monthly_fee") / F.sum("customer_id_avg_monthly_fee").over(w2)), 4).alias("customer_id_avg_monthly_fee_share"),
        F.rank().over(w3).alias("monthly_fee_rank"),
    )
)

with_dims = (
    scored_subscriptions
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.city"), F.col("p.credit_limit"))
)

daily = (
    with_dims
    .cube(
        F.when(F.col("customer_id_avg_monthly_fee") >= 243, "HIGH").otherwise("LOW").alias("customer_id_avg_monthly_fee_tier"),
    )
    .agg(
        F.count_if(F.col("city") == "New York").alias("new_york_events"),
        F.sum(F.when(F.col("plan") == "standard", F.col("customer_id_avg_monthly_fee_share")).otherwise(0)).alias("standard_customer_id_avg_monthly_fee_share"),
    )
    .filter(F.col("new_york_events") >= 1)
)

result = (
    daily
    .select(
        "customer_id_avg_monthly_fee_tier",
        "new_york_events",
        "standard_customer_id_avg_monthly_fee_share",
    )
    .orderBy(F.col("standard_customer_id_avg_monthly_fee_share").asc_nulls_first())
)
