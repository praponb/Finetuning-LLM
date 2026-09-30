from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "items",
        "ship_to",
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.coalesce(F.col("ship_to.city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
        F.array_contains("coupon_codes", "SPRING10").alias("used_spring10"),
    )
)
