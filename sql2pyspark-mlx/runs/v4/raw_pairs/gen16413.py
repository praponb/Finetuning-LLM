from pyspark.sql import functions as F

unpacked = (
    df_carts
    .filter(F.to_date("created_at") >= "2025-08-09")
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "items",
        "attributes",
        F.size(F.filter("items", lambda i: i["qty"] >= 3)).alias("bulk_lines"),
        F.transform("items", lambda i: i["product_id"]).alias("product_ids"),
        F.col("attributes").getItem("device").alias("device"),
        F.col("ship_to.city").alias("ship_city"),
        F.forall("items", lambda i: i["qty"] <= 5).alias("all_small_qty"),
    )
)

result = (
    unpacked
    .withColumn("bulk_lines_tier", F.when(F.col("bulk_lines") >= 3, "HIGH").otherwise("LOW"))
    .cube("bulk_lines_tier")
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("ship_city") == "Tokyo", 1)).alias("tokyo_rows"),
        F.percentile_approx("bulk_lines", 0.9).alias("bulk_lines_p90"),
        F.grouping("bulk_lines_tier").alias("grouping_level"),
    )
    .orderBy("bulk_lines_tier")
)
