from pyspark.sql import functions as F

parsed = (
    df_carts
    .filter(F.to_date("created_at") < "2025-06-01")
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.transform("items", lambda item: item["product_id"]).alias("product_ids"),
        F.array_contains("coupon_codes", "SPRING10").alias("used_spring10"),
    )
)

result = (
    parsed
    .groupBy(F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        F.min("created_at").alias("first_seen"),
        F.max("created_at").alias("last_seen"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") >= 1)
    .orderBy(F.col("distinct_ids").asc_nulls_last())
)
