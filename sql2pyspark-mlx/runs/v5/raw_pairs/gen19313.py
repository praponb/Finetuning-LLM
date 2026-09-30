from pyspark.sql import functions as F

with_dims = (
    df_orders
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.store_id",
        "e.status",
        "e.amount",
        "e.discount",
        F.col("c.city"),
        F.col("c.country"),
        F.col("c.email"),
    )
)

result = (
    with_dims
    .withColumn("discount_tier", F.when(F.col("discount") >= 0.38, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["discount_tier", "country"], ["discount_tier"]],
        "discount_tier",
        "country",
    )
    .agg(
        F.sum(F.when(F.col("status") == "SHIPPED", F.col("discount"))).alias("shipped_total_discount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_rows"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.grouping("discount_tier").alias("grouping_level"),
    )
    .orderBy("discount_tier")
)
