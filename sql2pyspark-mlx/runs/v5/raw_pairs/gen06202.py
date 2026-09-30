from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.orderBy("max_item_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("max_item_price", "id").rowsBetween(-6, -1)

parsed = (
    df_carts
    .select(
        "id",
        "created_at",
        "items",
        "attributes",
        F.array_contains("coupon_codes", "SPRING10").alias("used_spring10"),
        F.coalesce(F.col("attributes").getItem("utm_source"), F.lit("none")).alias("utm_source_or_default"),
        F.array_max(F.transform("items", lambda item: item["price"])).alias("max_item_price"),
    )
)

result = (
    parsed
    .select(
        "id",
        "created_at",
        "used_spring10",
        "utm_source_or_default",
        F.avg("max_item_price").over(w).alias("max_item_price_avg_30m"),
        F.sum("max_item_price").over(w2).alias("running_max_item_price"),
        F.avg("max_item_price").over(w3).alias("max_item_price_avg_6_prior"),
    )
)
