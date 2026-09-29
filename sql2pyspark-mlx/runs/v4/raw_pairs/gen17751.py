from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        F.col("attributes")["gift"].alias("gift"),
        F.exists("items", lambda li: li["price"] > 100).alias("has_premium_item"),
    )
)

result = (
    flattened_attrs
    .groupBy("customer_id")
    .agg(
        F.countDistinct("gift").alias("distinct_gifts"),
        F.countDistinct("gift").alias("distinct_gifts_2"),
        F.max("created_at").alias("last_seen"),
        F.max("created_at").alias("ended_at"),
        F.count("*").alias("num_rows"),
        F.countDistinct("gift").alias("distinct_gifts_3"),
    )
    .filter(F.col("distinct_gifts") > 1)
    .orderBy(F.col("distinct_gifts_3").desc())
)
