from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "items",
        "attributes",
        F.transform(
            F.filter("items", lambda i: i["promo"].isNotNull()),
            lambda i: F.struct(i["product_id"].alias("pid"), F.round(i["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.coalesce(F.col("attributes")["gift"], F.lit("direct")).alias("gift_or_default"),
        F.col("attributes").getItem("utm_source").alias("utm_source"),
        F.aggregate("items", F.lit(0.0), lambda acc, i: acc + i["qty"] * i["price"], lambda acc: F.round(acc, 2)).alias("basket_amount"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("basket_amount") >= 1240, "HIGH").otherwise("LOW").alias("basket_amount_tier"),
    )
    .agg(
        F.percentile_approx("basket_amount", 0.9).alias("basket_amount_p90"),
        F.percentile_approx("basket_amount", 0.5).alias("basket_amount_p50"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("basket_amount", 0.5).alias("basket_amount_p50_2"),
    )
    .orderBy(F.col("basket_amount_tier").desc())
)
