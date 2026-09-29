from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "items",
        F.size(F.filter("items", lambda li: li["qty"] >= 3)).alias("bulk_lines"),
        F.transform(
            F.filter("items", lambda li: li["promo"].isNotNull()),
            lambda li: F.struct(li["product_id"].alias("pid"), F.round(li["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.coalesce(F.col("attributes").getItem("gift"), F.lit("none")).alias("gift_or_default"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("none")).alias("utm_source_or_default"),
        F.size("items").alias("num_items"),
    )
)

result = (
    unpacked
    .groupBy("customer_id")
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.percentile_approx("bulk_lines", [0.25, 0.5, 0.75]).alias("bulk_lines_quartiles"),
        F.count("*").alias("cnt"),
        F.percentile_approx("bulk_lines", 0.5).alias("bulk_lines_p50"),
    )
    .filter(F.col("distinct_customer_ids") < 1)
    .orderBy(F.col("distinct_customer_ids").asc_nulls_first())
)
