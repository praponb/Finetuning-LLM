from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id")
w2 = Window.partitionBy("customer_id").orderBy("next_is_pending")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

filtered = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-04-04").cast("date")) & F.col("store_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "discount",
        F.trim("channel").alias("clean_channel"),
    )
)

base = (
    filtered
    .filter(F.col("discount") >= 0.38)
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "discount",
        "clean_channel",
        F.when(F.col("status") == "PENDING", 1).otherwise(0).alias("is_pending"),
    )
)

scored = (
    base
    .select(
        "*",
        F.lag("discount").over(w).alias("prev_discount"),
        F.lead("is_pending").over(w).alias("next_is_pending"),
        F.lag("is_pending").over(w).alias("prev_is_pending"),
        F.row_number().over(w).alias("event_seq"),
    )
)

ranked = (
    scored
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    ranked
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.email"), F.col("p.city"), F.col("p.country"))
)

result = (
    enriched
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "clean_channel",
        "is_pending",
        "prev_is_pending",
        "event_seq",
        "city",
        F.col("min_subscription_monthly_fee"),
    )
)
