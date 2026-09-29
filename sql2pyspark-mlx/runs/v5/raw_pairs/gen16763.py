from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("weight_kg").desc())
w2 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)

parsed = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.transform("items", lambda li: li["product_id"]).alias("product_ids"),
        F.col("ship_to.zip").alias("ship_zip"),
        F.col("attributes").getItem("gift").alias("gift"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.array_contains(F.map_keys("attributes"), "gift").alias("has_gift"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.cume_dist().over(w).alias("weight_kg_cume_dist"),
        F.sum("weight_kg").over(w2).alias("weight_kg_sum_15m"),
    )
)
