from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("city", "segment")
w4 = Window.partitionBy("promo")

flattened = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "li_item"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "pos",
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

metrics = (
    flattened
    .filter(~F.col("promo").isin(["SPRING10", "BUNDLE"]))
    .withColumn("price_share", F.round((F.col("price") / F.sum("price").over(w)), 4))
)

joined = (
    metrics
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select(
        "src.customer_id",
        "src.qty",
        "src.price",
        "src.promo",
        F.col("d.segment"),
        F.col("d.city"),
        F.col("d.signup_date"),
    )
)

with_windows = (
    joined
    .select(
        "promo",
        "signup_date",
        F.max("qty").over(w2).alias("city_max_qty"),
        F.round((F.col("price") / F.sum("price").over(w3)), 4).alias("price_share"),
        F.round((F.col("qty") / F.sum("qty").over(w)), 4).alias("qty_share"),
    )
)

metrics_carts = with_windows.withColumn("promo_max_price_share", F.max("price_share").over(w4))

result = (
    metrics_carts
    .select("city_max_qty", "price_share", "qty_share", "promo_max_price_share")
    .orderBy(F.col("price_share").asc_nulls_last())
)
