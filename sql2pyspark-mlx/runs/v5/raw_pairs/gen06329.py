from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id")
w2 = Window.partitionBy("plan", "city").orderBy(F.col("credit_limit").desc_nulls_first(), "id")
w_part = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("first_known_credit_limit_tier").orderBy(F.col("avg_credit_limit").desc())

deduped = (
    df_subscriptions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

valid = (
    deduped
    .filter(F.col("customer_id").isNotNull() & (F.col("plan") == "basic"))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        F.round((F.col("monthly_fee") * 0.85), 2).alias("monthly_fee_adjusted"),
    )
)

joined = (
    valid
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.credit_limit"), F.col("b.city"), F.col("b.age"))
)

filtered = (
    joined
    .filter(F.col("start_date") >= F.lit("2023-10-01").cast("date"))
    .withColumn("age_level", F.when(F.col("age") >= 65, "High").when(F.col("age") >= 26, "Medium").otherwise("Low"))
    .withColumn("resolved_city", F.coalesce("city", F.lit("NONE")))
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    ranked
    .filter(F.col("age").between(40, 47))
    .select(
        "id",
        "plan",
        "monthly_fee",
        "credit_limit",
        "age",
        "age_level",
        "resolved_city",
        F.count("monthly_fee_adjusted").over(w_part).alias("running_count_monthly_fee_adjusted"),
        F.sum("monthly_fee_adjusted").over(w_part).alias("running_sum_monthly_fee_adjusted"),
        F.min("monthly_fee_adjusted").over(w_part).alias("running_min_monthly_fee_adjusted"),
        F.max("monthly_fee_adjusted").over(w_part).alias("running_max_monthly_fee_adjusted"),
        F.first("credit_limit", ignorenulls=True).over(w4).alias("first_known_credit_limit"),
    )
)

cleaned = (
    scored
    .filter(F.col("plan") == "enterprise")
    .select(
        "id",
        "plan",
        "monthly_fee",
        "credit_limit",
        "age",
        "age_level",
        "resolved_city",
        "running_count_monthly_fee_adjusted",
        "running_sum_monthly_fee_adjusted",
        "running_min_monthly_fee_adjusted",
        "running_max_monthly_fee_adjusted",
        "first_known_credit_limit",
        F.when(F.col("running_sum_monthly_fee_adjusted") >= 320, "High").when(F.col("running_sum_monthly_fee_adjusted") >= 90, "Medium").otherwise("Low").alias("running_sum_monthly_fee_adjusted_band"),
        (F.col("monthly_fee") - F.col("age")).alias("monthly_fee_minus_age"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("first_known_credit_limit") >= 14900, "LARGE").otherwise("SMALL").alias("first_known_credit_limit_tier"),
        "plan",
        F.when(F.col("running_min_monthly_fee_adjusted") >= 79, "LARGE").otherwise("SMALL").alias("running_min_monthly_fee_adjusted_tier"),
    )
    .agg(
        F.count_if(F.col("plan") == "premium").alias("premium_events"),
        F.percentile_approx("running_min_monthly_fee_adjusted", 0.5).alias("running_min_monthly_fee_adjusted_p50"),
        F.avg("credit_limit").alias("avg_credit_limit"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
    .withColumn("avg_credit_limit_dense_rank", F.dense_rank().over(w5))
)
