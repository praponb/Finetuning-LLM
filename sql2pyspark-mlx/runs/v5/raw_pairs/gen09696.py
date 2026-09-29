from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status")
w2 = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy(F.col("credit_limit").desc_nulls_last())

filtered = (
    df_subscriptions
    .filter(F.col("start_date") >= F.lit("2023-07-22").cast("date"))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.when(F.col("monthly_fee") >= 265, "High").when(F.col("monthly_fee") >= 57, "Medium").otherwise("Low").alias("monthly_fee_level"),
        F.date_add("start_date", 60).alias("start_date_plus_60d"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4).alias("monthly_fee_share"),
        F.sum("monthly_fee").over(w2).alias("cumulative_monthly_fee"),
    )
)

enriched = (
    scored
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.end_date",
        "e.monthly_fee",
        "e.status",
        "e.start_date_plus_60d",
        "e.monthly_fee_share",
        "e.cumulative_monthly_fee",
        F.col("b.name"),
        F.col("b.city"),
    )
)

matched_keys = (
    enriched
    .select("status", "customer_id")
    .intersect(
        df_subscriptions
        .filter(F.col("plan").isin(["premium", "basic", "enterprise"]))
        .select("status", "customer_id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.city").isin(["Berlin", "Chiang Mai", "New York"]))
    .select("a.*", F.col("p.credit_limit"), F.col("p.segment"), F.col("p.age"))
)

metrics = joined.select("*", F.dense_rank().over(w3).alias("credit_limit_dense_rank"))

valid = (
    metrics
    .filter(F.col("monthly_fee") <= 190)
    .withColumn("credit_limit_dense_rank_adjusted", F.round((F.col("credit_limit_dense_rank") * 1.07), 2))
    .withColumn("status_upper", F.upper("status"))
)

result = (
    valid
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "status",
        "monthly_fee_share",
        "cumulative_monthly_fee",
        "name",
        "city",
        "segment",
        F.when(F.col("status").rlike("^(CANCELLED|EXPIRED)$"), "GROUP_B").when(F.col("status").rlike("^(ACTIVE|PAUSED)$"), "PRIORITY").otherwise("EXTENDED").alias("status_zone"),
    )
    .orderBy(F.col("status").desc())
)
