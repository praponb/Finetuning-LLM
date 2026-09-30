from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w_grp = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_orders
    .filter(F.col("discount") < 0.12)
    .select(
        "*",
        F.max("discount").over(w).alias("status_max_discount"),
        F.sum("amount").over(w_grp).alias("running_amount"),
    )
)

filtered = (
    with_windows
    .filter(F.col("customer_id").isNotNull() & (F.col("order_date") > F.lit("2023-05-18").cast("date")))
    .select("*", F.round((F.col("discount") * 100), 2).alias("discount_scaled"))
)

remaining_keys = (
    filtered
    .select("channel", "customer_id")
    .intersect(
        df_orders
        .filter(F.col("discount").between(0.13, 0.34))
        .select("channel", "customer_id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "src.customer_id",
        "src.store_id",
        "src.order_date",
        "src.channel",
        "src.amount",
        "src.status_max_discount",
        "src.running_amount",
        "src.discount_scaled",
        F.col("b.signup_date"),
    )
)

attributed = (
    joined
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & F.col("s.signup_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    attributed
    .rollup(F.date_trunc("MONTH", "signup_date").alias("activity_month"), "store_id")
    .agg(
        F.percentile_approx("discount_scaled", 0.95).alias("discount_scaled_p95"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
    )
    .filter(F.col("discount_scaled_p95") > 0)
)
