from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("monthly_fee_int_times_monthly_fee_int_filled").asc_nulls_first())
w2 = Window.partitionBy("city").orderBy(F.col("monthly_fee_int_times_monthly_fee_int_filled").desc_nulls_first())
w3 = Window.partitionBy("customer_id", "city").orderBy(F.col("monthly_fee_int").desc_nulls_first())

cleaned = (
    df_subscriptions
    .filter(F.col("customer_id").isin([11, 20]))
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "status",
        F.initcap(F.trim("plan")).alias("plan_title"),
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
    )
)

remaining_keys = (
    cleaned
    .select("status", "id")
    .intersectAll(
        df_subscriptions
        .filter(F.col("monthly_fee").isNotNull())
        .select("status", "id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("customer_id").isNotNull())
    .withColumn("monthly_fee_int_filled", F.coalesce("monthly_fee_int", F.lit(0)))
    .withColumn("status_upper", F.upper("status"))
)

with_dims = (
    filtered
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .select(
        "src.customer_id",
        "src.end_date",
        "src.monthly_fee_int",
        "src.monthly_fee_int_filled",
        F.col("c.city"),
        F.col("c.name"),
        F.col("c.credit_limit"),
    )
)

cleaned_subscriptions = (
    with_dims
    .filter(F.col("end_date") > "2024-07-01")
    .select(
        "customer_id",
        "end_date",
        "monthly_fee_int",
        "monthly_fee_int_filled",
        "city",
        "name",
        "credit_limit",
        F.upper("name").alias("name_upper"),
        F.round("credit_limit", 0).alias("credit_limit_rounded"),
    )
)

base = (
    cleaned_subscriptions
    .filter((F.col("end_date") > F.to_date(F.lit("2023-11-08"))) & (~F.col("city").eqNullSafe(F.lit("Tokyo"))))
    .select(
        "customer_id",
        "monthly_fee_int",
        "city",
        "name",
        "credit_limit",
        "name_upper",
        "credit_limit_rounded",
        (F.col("monthly_fee_int") * F.col("monthly_fee_int_filled")).alias("monthly_fee_int_times_monthly_fee_int_filled"),
    )
)

kept_keys = (
    base
    .select("customer_id")
    .intersect(
        df_subscriptions
        .filter(F.col("plan").isin(["premium", "standard"]))
        .select("customer_id")
    )
)

verified = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "customer_id",
        "city",
        "name",
        "name_upper",
        "credit_limit_rounded",
        F.dense_rank().over(w).alias("monthly_fee_int_times_monthly_fee_int_filled_dense_rank"),
        F.percent_rank().over(w2).alias("monthly_fee_int_times_monthly_fee_int_filled_percent_rank"),
        F.cume_dist().over(w3).alias("monthly_fee_int_cume_dist"),
    )
)
