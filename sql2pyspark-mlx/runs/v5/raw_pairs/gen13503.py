from pyspark.sql import functions as F

lines = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("value"))

result = (
    lines
    .select(
        "id",
        "value",
        F.when(F.col("value").rlike("^(BUNDLE|VIP)$"), "GROUP_B").otherwise("GROUP_A").alias("value_zone"),
    )
    .orderBy(F.col("value_zone").desc())
)
