from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)

parsed = (
    df_carts
    .select(
        "id",
        "created_at",
        "ship_to",
        "attributes",
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.exists("items", lambda x: x["price"] > 100).alias("has_premium_item"),
        F.array_contains("coupon_codes", "SPRING10").alias("used_spring10"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.last("weight_kg", ignorenulls=True).over(w_part).alias("last_known_weight_kg"),
        F.count("weight_kg").over(w2).alias("weight_kg_count_30m"),
    )
)
