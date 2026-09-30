from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy(F.col("credit_limit").desc_nulls_first())
w3 = Window.partitionBy("city", "country")
w4 = Window.partitionBy("city").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("segment", "order_status").orderBy("age_share", "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())

with_last = (
    df_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("order_status"), F.col("c.shipping_fee"))
)

scored = (
    with_last
    .filter(F.col("country") == "SG")
    .withColumn("credit_limit_rank", F.rank().over(w2))
    .withColumn("age_share", F.round((F.col("age") / F.sum("age").over(w3)), 4))
    .withColumn("last_known_shipping_fee", F.last("shipping_fee", ignorenulls=True).over(w4))
    .withColumn("first_known_order_status", F.first("order_status", ignorenulls=True).over(w_recent))
)

latest = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

eligible = (
    latest
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.end_date") >= F.col("s.signup_date")), "left_anti")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.email",
        "s.country",
        "s.is_active",
        "s.shipping_fee",
        "s.age_share",
        F.col("c.monthly_fee"),
        F.col("c.status"),
    )
)

summary = (
    with_latest
    .withColumn("shipping_fee_tier", F.when(F.col("shipping_fee") >= 37, "HIGH").otherwise("LOW"))
    .withColumn("age_share_tier", F.when(F.col("age_share") >= 0.7, "PREMIUM").otherwise("STANDARD"))
    .rollup("shipping_fee_tier", "age_share_tier")
    .agg(
        F.max(F.when(F.col("status") == "ACTIVE", F.col("monthly_fee"))).alias("active_max_monthly_fee"),
        F.round(F.sum("shipping_fee") / F.nullif(F.count(F.when(F.col("country") == "DE", 1)), F.lit(0)), 2).alias("shipping_fee_per_de"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    summary
    .select(
        "shipping_fee_tier",
        "age_share_tier",
        "active_max_monthly_fee",
        "row_count",
        "de_count",
    )
    .orderBy(F.col("shipping_fee_tier").desc_nulls_last())
)
