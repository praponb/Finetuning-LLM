from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("customer_id", "status")
w3 = Window.partitionBy("plan").orderBy(F.col("customer_id_min_monthly_fee").desc(), "id")
w4 = Window.partitionBy("status").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("plan").orderBy(F.col("monthly_fee_share").desc(), "id")
w6 = Window.partitionBy("customer_id").orderBy("monthly_fee_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2023-05-04").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.round((F.col("monthly_fee") * 1.15), 2).alias("monthly_fee_adjusted"),
    )
)

metrics = (
    base
    .filter(F.col("monthly_fee").between(70, 160))
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4).alias("monthly_fee_share"),
        F.min("monthly_fee").over(w2).alias("customer_id_min_monthly_fee"),
    )
)

deduped = (
    metrics
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

enriched = (
    deduped
    .select(
        "*",
        F.sum("customer_id_min_monthly_fee").over(w4).alias("running_customer_id_min_monthly_fee"),
        F.ntile(10).over(w5).alias("monthly_fee_share_bucket"),
        F.first("monthly_fee_share", ignorenulls=True).over(w6).alias("first_known_monthly_fee_share"),
    )
)

valid = (
    enriched
    .filter((F.col("end_date") > F.lit("2023-07-31").cast("date")) & (F.col("monthly_fee") < 230))
    .select(
        "*",
        F.round("monthly_fee_share_bucket", 2).alias("monthly_fee_share_bucket_rounded"),
        F.round("row_num", 2).alias("row_num_rounded"),
    )
)

stats = (
    valid
    .withColumn("running_customer_id_min_monthly_fee_tier", F.when(F.col("running_customer_id_min_monthly_fee") >= 1310, "PREMIUM").otherwise("STANDARD"))
    .rollup("running_customer_id_min_monthly_fee_tier")
    .agg(
        F.percentile_approx("row_num_rounded", [0.25, 0.5, 0.75]).alias("row_num_rounded_quartiles"),
        F.min("first_known_monthly_fee_share").alias("min_first_known_monthly_fee_share"),
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_count"),
        F.count_if(F.col("plan") == "standard").alias("standard_events"),
        F.grouping("running_customer_id_min_monthly_fee_tier").alias("grouping_level"),
    )
    .filter(F.col("min_first_known_monthly_fee_share") < 1000000)
)

result = (
    stats
    .select(
        "running_customer_id_min_monthly_fee_tier",
        "row_num_rounded_quartiles",
        "min_first_known_monthly_fee_share",
        "standard_events",
        "grouping_level",
    )
    .orderBy(F.col("standard_events").desc_nulls_last(), F.col("grouping_level").desc())
)
