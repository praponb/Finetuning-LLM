from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "ship_to",
        F.col("ship_to.zip").alias("ship_zip"),
        F.coalesce(F.col("ship_to.zip"), F.lit("UNKNOWN")).alias("zip_or_unknown"),
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.coalesce(F.col("attributes").getItem("device"), F.lit("none")).alias("device_or_default"),
        F.forall("items", lambda i: i["qty"] <= 5).alias("all_small_qty"),
    )
)
