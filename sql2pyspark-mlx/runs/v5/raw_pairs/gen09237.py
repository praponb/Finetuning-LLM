from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("coupon_codes_count").desc_nulls_first(), "id", "attr_key")
w2 = Window.partitionBy("attr_key", "customer_id").orderBy(F.col("coupon_codes_count").desc())
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("amount").alias("max_order_amount"))

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "v"),
    )
)

cleaned = (
    exploded
    .filter(F.col("attr_key").isin(["weight_kg", "device", "gift"]))
    .withColumn("coupon_codes_count", F.size("coupon_codes"))
    .withColumn("is_spring10", F.array_contains("coupon_codes", "SPRING10"))
)

ranked = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "attr_key",
        "v",
        "coupon_codes_count",
        "is_spring10",
    )
)

deduped = (
    ranked
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "attr_key",
        "coupon_codes_count",
        "is_spring10",
        "latest_rank",
        F.col("max_order_amount"),
    )
    .orderBy("coupon_codes_count", "attr_key")
)
