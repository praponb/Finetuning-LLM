from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "ship_to",
        "attributes",
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.col("attributes").getItem("weight_kg").try_cast("double").alias("weight_kg"),
        F.coalesce(F.col("ship_to.zip"), F.lit("UNKNOWN")).alias("zip_or_unknown"),
        F.array_max(F.transform("items", lambda item: item["price"])).alias("max_item_price"),
    )
)
