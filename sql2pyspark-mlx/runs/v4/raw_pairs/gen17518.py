from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("qty").desc_nulls_first())
w_grp = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("promo")

lines = df_carts.select("id", "created_at", F.inline("items"))

with_windows = (
    lines
    .withColumn("qty_rank", F.rank().over(w))
    .withColumn("price_count_1h", F.count("price").over(w_grp))
    .withColumn("product_id_share", F.round((F.col("product_id") / F.sum("product_id").over(w3)), 4))
)

matched_keys = (
    with_windows
    .select("id")
    .intersect(
        df_carts
        .filter(F.col("customer_id").isNotNull())
        .select("id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("created_at") > F.lit("2025-03-01").cast("date"))
    .select(
        "id",
        "created_at",
        "product_id",
        "qty",
        "price",
        "qty_rank",
        "product_id_share",
        (F.col("qty_rank") - F.col("product_id_share")).alias("qty_rank_minus_product_id_share"),
        F.coalesce("promo", F.lit("UNKNOWN")).alias("promo_filled"),
    )
)

result = (
    valid
    .groupBy(
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
        F.when(F.col("product_id") >= 8, "LARGE").otherwise("SMALL").alias("product_id_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg("qty_rank_minus_product_id_share").alias("avg_qty_rank_minus_product_id_share"),
    )
    .filter(F.col("cnt") >= 1)
    .orderBy(F.col("cnt").asc_nulls_last())
)
