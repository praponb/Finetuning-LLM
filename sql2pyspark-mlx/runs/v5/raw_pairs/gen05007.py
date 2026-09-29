from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("created_at", "id", "line_no")
w2 = Window.partitionBy("customer_id").orderBy("created_at", "id", "line_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("product_id", "id", "line_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("island_id", "id", "line_no").rowsBetween(-6, Window.currentRow)
w_grp = Window.partitionBy("country").orderBy(F.col("monthly_fee").desc_nulls_last())
w6 = Window.partitionBy("plan", "promo")
w7 = Window.partitionBy("promo").orderBy("created_at", "id", "line_no", "subscription_id")
w8 = Window.partitionBy("promo").orderBy("created_at", "id", "line_no", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

exploded = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "elem"))
    .select(
        "id",
        "customer_id",
        "created_at",
        (F.col("pos") + 1).alias("line_no"),
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

flagged = (
    exploded
    .withColumn("is_new_burst", F.when(F.lag("created_at").over(w).isNull() | (F.col("created_at") > F.lag("created_at").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessions_carts = flagged.withColumn("island_id", F.sum("is_new_burst").over(w2))

metrics = (
    sessions_carts
    .select(
        "*",
        F.sum("price").over(w3).alias("cumulative_price"),
        F.avg("island_id").over(w4).alias("island_id_avg_6"),
    )
)

enriched = (
    metrics
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.email"), F.col("p.signup_date"), F.col("p.country"))
)

matched = (
    enriched
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & F.col("a.created_at").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

enriched_carts = (
    matched
    .select(
        "id",
        "customer_id",
        "created_at",
        "line_no",
        "product_id",
        "qty",
        "promo",
        "island_id",
        "email",
        "signup_date",
        "country",
        "subscription_id",
        F.percent_rank().over(w_grp).alias("monthly_fee_percent_rank"),
        F.min("price").over(w6).alias("plan_min_price"),
    )
)

gaps = (
    enriched_carts
    .withColumn("new_group_flag", F.when(F.col("created_at") - F.lag("created_at").over(w7) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id_2", F.sum("new_group_flag").over(w8))

result = (
    sessionized
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "created_at",
        "line_no",
        "product_id",
        "promo",
        "island_id",
        "email",
        "country",
        F.col("sum_account_balance"),
    )
)
