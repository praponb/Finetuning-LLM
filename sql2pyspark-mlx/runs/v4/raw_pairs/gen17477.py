from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        F.col("attributes")["device"].alias("device"),
        F.aggregate("items", F.lit(0.0), lambda acc, li: acc + li["qty"] * li["price"], lambda acc: F.round(acc, 2)).alias("basket_amount"),
        F.coalesce(F.col("attributes").getItem("device"), F.lit("none")).alias("device_or_default"),
        F.coalesce(F.col("ship_to.zip"), F.lit("UNKNOWN")).alias("zip_or_unknown"),
        F.col("attributes")["gift"].alias("gift"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("basket_amount") >= 1420, "PREMIUM").otherwise("STANDARD").alias("basket_amount_tier"),
    )
    .agg(
        F.percentile_approx("basket_amount", 0.5).alias("basket_amount_p50"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("basket_amount"), 2).alias("avg_basket_amount"),
    )
)
