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
    .select("id", "created_at")
    .orderBy(F.col("created_at").desc())
)
