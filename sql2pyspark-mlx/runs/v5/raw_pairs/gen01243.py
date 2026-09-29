from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("qty").asc_nulls_first())
w2 = Window.partitionBy("promo")
w3 = Window.orderBy(F.col("product_id").asc_nulls_last())
w4 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w5 = Window.partitionBy("customer_id")
w6 = Window.partitionBy("promo").orderBy(F.col("qty_share").asc_nulls_last())

flattened = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.promo"),
    )
)

metrics = (
    flattened
    .select(
        "id",
        "customer_id",
        "qty",
        "promo",
        F.cume_dist().over(w).alias("qty_cume_dist"),
        F.round((F.col("qty") / F.sum("qty").over(w2)), 4).alias("qty_share"),
        F.cume_dist().over(w3).alias("product_id_cume_dist"),
        F.max("product_id").over(w4).alias("product_id_max_30m"),
        F.sum("qty").over(w5).alias("customer_id_sum_qty"),
    )
)

metrics_carts = metrics.withColumn("qty_share_percent_rank", F.percent_rank().over(w6))

enriched = (
    metrics_carts
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .filter(F.col("d.is_active"))
    .select(
        "src.id",
        "src.customer_id",
        "src.qty",
        "src.promo",
        "src.qty_cume_dist",
        "src.qty_share_percent_rank",
        F.col("d.age"),
        F.col("d.city"),
    )
)

result = (
    enriched
    .select(
        "id",
        "qty",
        "promo",
        "qty_cume_dist",
        "age",
        "city",
        F.when(F.col("city").rlike("^(Singapore|Bangkok)$"), "EXTENDED").otherwise("PRIORITY").alias("city_zone"),
    )
    .orderBy(F.col("promo").desc())
)
