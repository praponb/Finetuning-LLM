from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        "attributes",
        F.size("items").alias("num_items"),
        F.aggregate("items", F.lit(0.0), lambda acc, li: acc + li["qty"] * li["price"], lambda acc: F.round(acc, 2)).alias("basket_amount"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("num_items") >= 3, "LARGE").otherwise("SMALL").alias("num_items_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.round(F.avg("basket_amount"), 2).alias("avg_basket_amount"),
        F.percentile_approx("basket_amount", 0.5).alias("basket_amount_p50"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
    )
)
