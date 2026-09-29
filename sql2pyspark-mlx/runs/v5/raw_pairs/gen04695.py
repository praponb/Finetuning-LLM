from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("value").orderBy(F.col("created_at_hour").desc_nulls_last())

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "v"),
    )
)

exploded = flattened.select("created_at", "v", F.explode("coupon_codes").alias("value"))

filtered = (
    exploded
    .filter(F.hour("created_at").between(9, 14))
    .select(
        "*",
        F.quarter("created_at").alias("created_at_quarter"),
        F.hour("created_at").alias("created_at_hour"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w).alias("created_at_hour_dense_rank"),
        F.percent_rank().over(w).alias("created_at_hour_percent_rank"),
    )
)

result = (
    enriched
    .select(
        "created_at",
        "v",
        "value",
        "created_at_quarter",
        "created_at_hour",
        "created_at_hour_dense_rank",
        F.when(F.col("value").rlike("^(VIP|WELCOME5)$"), "PRIORITY").when(F.col("value").rlike("^(SPRING10|BUNDLE)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("value_zone"),
    )
    .orderBy(F.col("created_at_hour_dense_rank").desc())
)
