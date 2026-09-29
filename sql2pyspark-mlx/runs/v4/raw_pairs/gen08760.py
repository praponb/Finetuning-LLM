from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "customer_id").orderBy(F.col("product_id").asc_nulls_last(), "id", "pos", "subscription_id")

lines = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "li_item"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
    )
)

matched = (
    lines
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.created_at") >= F.col("sub.start_date")) & (F.col("e.created_at") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

deduped = (
    matched
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .groupBy(
        "plan",
        F.when(F.col("product_id") >= 9, "HIGH").otherwise("LOW").alias("product_id_tier"),
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
    )
    .agg(
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.percentile_approx("product_id", 0.95).alias("product_id_p95"),
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
    )
    .orderBy(F.col("plan").desc())
)
