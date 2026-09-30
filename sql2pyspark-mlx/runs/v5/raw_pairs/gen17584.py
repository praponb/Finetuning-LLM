from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "ship_to",
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.size("attributes").alias("num_attributes"),
    )
)
