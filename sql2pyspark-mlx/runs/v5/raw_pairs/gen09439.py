from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "created_at",
        "ship_to",
        "attributes",
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
        F.col("ship_to.city").alias("ship_city"),
        F.array_max(F.transform("items", lambda item: item["price"])).alias("max_item_price"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("max_item_price") >= 192, "PREMIUM").otherwise("STANDARD").alias("max_item_price_tier"),
        "ship_city",
    )
    .agg(
        F.min("created_at").alias("first_created_at"),
        F.round(F.avg("max_item_price"), 2).alias("avg_max_item_price"),
        F.sum(F.when(F.col("ship_city") == "Tokyo", F.col("bulk_lines")).otherwise(0)).alias("tokyo_bulk_lines"),
        F.count(F.when(F.col("ship_city") == "Singapore", 1)).alias("singapore_rows"),
    )
    .orderBy(F.col("avg_max_item_price").asc_nulls_first())
)
