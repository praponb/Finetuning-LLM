from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("coupon_codes").alias("element"),
    )
)

result = (
    lines
    .select(
        "id",
        "element",
        F.when(F.col("element").rlike("^(WELCOME5|SPRING10)$"), "GROUP_A").when(F.col("element").rlike("^(VIP|BUNDLE)$"), "CORE").otherwise("PRIORITY").alias("element_zone"),
    )
)
