from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "items",
        "attributes",
        F.transform("items", lambda li: li["product_id"]).alias("product_ids"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.forall("items", lambda li: li["qty"] <= 5).alias("all_small_qty"),
        F.col("ship_to.zip").alias("ship_zip"),
        F.coalesce(F.col("ship_to.country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
    )
)
