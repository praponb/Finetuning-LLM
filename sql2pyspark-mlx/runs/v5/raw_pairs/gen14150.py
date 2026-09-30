from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

result = (
    exploded
    .select(
        "id",
        "element",
        F.when(F.col("element").rlike("^(VIP|BUNDLE)$"), "EXTENDED").otherwise("GROUP_A").alias("element_zone"),
    )
    .orderBy(F.col("id").desc_nulls_last())
)
