from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("product_id", "id", "pos").rowsBetween(-6, -1)
w2 = Window.partitionBy("promo")
w3 = Window.orderBy("promo_min_pos", "id", "pos").rowsBetween(-2, Window.currentRow)
w_running = Window.partitionBy("promo").orderBy("product_id", "id", "pos").rowsBetween(-2, Window.currentRow)
w_ordered = Window.partitionBy("promo").orderBy("product_id_avg_6_prior", "id", "pos").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "li_item"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("li_item.product_id"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

metrics = (
    lines
    .select(
        "*",
        F.avg("product_id").over(w).alias("product_id_avg_6_prior"),
        F.min("pos").over(w2).alias("promo_min_pos"),
    )
)

valid = (
    metrics
    .filter(F.hour("created_at").between(12, 17))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "pos",
        "product_id",
        "price",
        "promo",
        "product_id_avg_6_prior",
        "promo_min_pos",
        F.to_date("created_at").alias("created_at_date"),
    )
)

enriched = (
    valid
    .select(
        "*",
        F.avg("promo_min_pos").over(w3).alias("promo_min_pos_avg_2"),
        F.avg("price").over(w_running).alias("price_avg_2"),
        F.sum("product_id").over(w_ordered).alias("running_product_id"),
    )
)

result = (
    enriched
    .select(
        "id",
        "coupon_codes",
        "pos",
        "product_id",
        "price",
        "promo",
        "product_id_avg_6_prior",
        "promo_min_pos",
        "created_at_date",
    )
)
