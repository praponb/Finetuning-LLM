from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")

lines = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

metrics = (
    lines
    .select(
        "*",
        F.sum("price").over(w).alias("promo_sum_price"),
        F.round((F.col("qty") / F.sum("qty").over(w)), 4).alias("qty_share"),
    )
)

base = (
    metrics
    .filter((F.col("created_at") >= F.lit("2025-03-02").cast("date")) & F.col("promo").isNotNull() & F.hour("created_at").between(10, 13))
    .withColumn("promo_sum_price_level", F.when(F.col("promo_sum_price") >= 908, "High").when(F.col("promo_sum_price") >= 173, "Medium").otherwise("Low"))
    .withColumn("greatest_qty_share_qty", F.greatest("qty_share", "qty"))
    .withColumn("promo_filled", F.coalesce("promo", F.lit("NONE")))
)

result = (
    base
    .select("promo", "price", "greatest_qty_share_qty", "qty_share")
    .unpivot(
        ["promo"],
        ["price", "greatest_qty_share_qty", "qty_share"],
        "metric_name",
        "value",
    )
    .filter(F.col("value").isNotNull())
)
