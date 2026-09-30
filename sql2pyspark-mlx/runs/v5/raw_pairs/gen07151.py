from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("value").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("value")

flattened = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.price"),
    )
)

lines = (
    flattened
    .select(
        "id",
        "created_at",
        "product_id",
        "price",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

base = (
    lines
    .filter(F.col("value").isNotNull() & (~F.col("value").eqNullSafe(F.lit("WELCOME5"))))
    .select(
        "id",
        "created_at",
        "product_id",
        "value",
        F.initcap(F.trim("value")).alias("value_title"),
        F.coalesce("value", F.lit("DIRECT")).alias("value_filled"),
    )
)

kept_keys = (
    base
    .select("id")
    .intersect(
        df_carts
        .filter(F.array_contains("coupon_codes", "SPRING10"))
        .select("id")
    )
)

verified = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .select(
        "*",
        F.max("product_id").over(w).alias("product_id_max_1h"),
        F.round((F.col("product_id") / F.sum("product_id").over(w2)), 4).alias("product_id_share"),
    )
)

result = with_windows.select("id", "product_id", "value", "value_title", "value_filled")
