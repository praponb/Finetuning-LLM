from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("end_date").desc())
shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("shipping_fee").alias("max_order_shipping_fee"))

valid = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2023-03-29").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        "status",
        F.substring("plan", 1, 3).alias("plan_prefix"),
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
    )
)

deduped = (
    valid
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

kept_keys = (
    deduped
    .select("status", "customer_id")
    .subtract(
        df_subscriptions
        .filter(F.month("end_date").isin([3, 7, 12]))
        .select("status", "customer_id")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

valid_subscriptions = (
    kept_rows
    .filter((F.col("end_date") > F.lit("2023-12-05").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        "status",
        "plan_prefix",
        "monthly_fee_int",
        F.last_day("end_date").alias("end_date_month_end"),
    )
)

joined = (
    valid_subscriptions
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.country"), F.col("b.name"))
)

result = (
    joined
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        "plan_prefix",
        "country",
        "name",
        F.col("max_order_shipping_fee"),
    )
    .orderBy(F.col("country").desc())
)
