from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "v"),
    )
)

lines = exploded.select("id", "v", F.explode_outer("coupon_codes").alias("value"))

valid = (
    lines
    .filter(F.col("value").isin(["WELCOME5", "SPRING10"]))
    .select(
        "*",
        F.when(F.col("value") == "SPRING10", 1).otherwise(0).alias("is_spring10"),
        F.upper("value").alias("value_upper"),
    )
)

result = (
    valid
    .select("id", "value", "is_spring10", "value_upper")
    .orderBy(F.col("id").desc_nulls_first())
)
