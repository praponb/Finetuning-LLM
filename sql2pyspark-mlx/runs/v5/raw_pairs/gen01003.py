from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("city")

lines = (
    df_carts
    .withColumn("entry", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("entry.product_id"),
        F.col("entry.qty"),
        F.col("entry.price"),
        F.col("entry.promo"),
    )
)

enriched = (
    lines
    .select(
        "*",
        F.round((F.col("qty") / F.sum("qty").over(w)), 4).alias("qty_share"),
        F.round((F.col("product_id") / F.sum("product_id").over(w)), 4).alias("product_id_share"),
    )
)

enriched_carts = (
    enriched
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select(
        "src.id",
        "src.customer_id",
        "src.created_at",
        "src.qty",
        "src.price",
        "src.promo",
        "src.qty_share",
        "src.product_id_share",
        F.col("c.segment"),
        F.col("c.city"),
    )
)

metrics = (
    enriched_carts
    .filter(F.col("qty_share") < 0.57)
    .withColumn("city_avg_product_id_share", F.avg("product_id_share").over(w2))
)

result = (
    metrics
    .select(
        "id",
        "customer_id",
        "created_at",
        "qty",
        "product_id_share",
        "city",
        "city_avg_product_id_share",
    )
    .orderBy(F.col("city_avg_product_id_share").asc_nulls_last())
)
