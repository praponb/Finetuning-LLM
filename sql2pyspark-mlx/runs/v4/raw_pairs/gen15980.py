from pyspark.sql import functions as F

lines = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("element"))

result = (
    lines
    .select(
        "id",
        "element",
        F.when(F.col("element").rlike("^(BUNDLE|WELCOME5)$"), "PRIORITY").when(F.col("element").rlike("^(SPRING10|VIP)$"), "CORE").otherwise("EXTENDED").alias("element_zone"),
    )
    .orderBy(F.col("element_zone").desc())
)
