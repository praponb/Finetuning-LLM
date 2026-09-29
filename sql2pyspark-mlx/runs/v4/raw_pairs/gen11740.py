from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("subscription_id_tier").orderBy(F.col("distinct_customer_ids").desc())

matched = (
    df_orders
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & F.col("s.order_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "inner")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    matched
    .groupBy(
        F.when(F.col("subscription_id") >= 10, "LARGE").otherwise("SMALL").alias("subscription_id_tier"),
        F.when(F.col("amount") >= 1350, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.corr("shipping_fee", "discount").alias("corr_shipping_fee_discount"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.percentile_approx("discount", [0.25, 0.5, 0.75]).alias("discount_quartiles"),
    )
    .withColumn("distinct_customer_ids_dense_rank", F.dense_rank().over(w))
)
