from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("signup_date", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id", "attr_key")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "v"),
    )
)

cleaned = (
    flattened
    .filter(~F.col("attr_key").eqNullSafe(F.lit("utm_source")))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "attr_key",
        "v",
        F.array_join("coupon_codes", ", ").alias("coupon_codes_text"),
        F.coalesce("attr_key", F.lit("UNKNOWN")).alias("attr_key_clean"),
    )
)

joined = (
    cleaned
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .filter(F.col("p.is_active"))
    .select("s.*", F.col("p.country"), F.col("p.credit_limit"), F.col("p.signup_date"))
)

metrics = (
    joined
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "attr_key",
        "v",
        "attr_key_clean",
        "country",
        F.max("credit_limit").over(w).alias("running_max_credit_limit"),
        F.sum("credit_limit").over(w).alias("running_sum_credit_limit"),
        F.avg("credit_limit").over(w).alias("running_avg_credit_limit"),
        F.max("credit_limit").over(w2).alias("customer_id_max_credit_limit"),
    )
)

valid = (
    metrics
    .filter(F.col("running_max_credit_limit") >= 9600)
    .select(
        "*",
        F.when(F.col("running_sum_credit_limit") >= 71000, "High").when(F.col("running_sum_credit_limit") >= 19000, "Medium").otherwise("Low").alias("running_sum_credit_limit_level"),
    )
)

result = (
    valid
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "attr_key",
        "v",
        "attr_key_clean",
        "running_max_credit_limit",
        "running_sum_credit_limit",
        "customer_id_max_credit_limit",
        F.when(F.col("attr_key").rlike("^(utm_source|gift)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("attr_key_zone"),
        F.col("avg_subscription_monthly_fee"),
    )
    .orderBy(F.col("id").desc_nulls_last())
)
