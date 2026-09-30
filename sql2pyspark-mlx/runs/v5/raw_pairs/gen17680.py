from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "value"),
    )
)

lines_carts = lines.select("id", "k", "value", F.explode("coupon_codes").alias("value_2"))

cleaned = (
    lines_carts
    .filter(F.col("value_2").isNotNull())
    .withColumn("value_2_upper", F.upper("value_2"))
    .withColumn("k_filled", F.coalesce("k", F.lit("OTHER")))
)

cleaned_carts = (
    cleaned
    .filter(~F.col("value_2").isin(["SPRING10", "VIP"]))
    .select(
        "id",
        "k",
        "value",
        "value_2",
        "value_2_upper",
        "k_filled",
        F.concat_ws("-", "k", "value_2").alias("k_value_2_key"),
        F.coalesce("value_2", F.lit("OTHER")).alias("value_2_filled"),
    )
)

result = cleaned_carts.select("id", "k", "value", "value_2", "k_filled", "value_2_filled")
