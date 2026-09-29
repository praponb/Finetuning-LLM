from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        F.transform(
            F.filter("items", lambda i: i["promo"].isNotNull()),
            lambda i: F.struct(i["product_id"].alias("pid"), F.round(i["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.array_contains(F.map_keys("attributes"), "gift").alias("has_gift"),
        F.coalesce(F.col("attributes")["gift"], F.lit("unknown")).alias("gift_or_default"),
        F.transform("items", lambda i: i["product_id"]).alias("product_ids"),
    )
)

result = (
    flattened_attrs
    .groupBy(F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.min("created_at").alias("first_seen"),
        F.max("created_at").alias("last_seen"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min_2"),
        F.max("created_at").alias("last_created_at"),
    )
    .filter(F.col("duration_min_2") > 0)
    .orderBy(F.col("duration_min").asc_nulls_last())
)
