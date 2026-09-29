from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("greatest_price_qty").desc_nulls_first())
w2 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.orderBy(F.col("greatest_price_qty_count_2h").desc_nulls_first())
w5 = Window.orderBy(F.col("greatest_price_qty").asc_nulls_first())
w6 = Window.orderBy("greatest_price_qty_count_2h_max_30m")
w7 = Window.orderBy(F.col("greatest_price_qty_count_2h_max_30m").desc_nulls_first())

flattened = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
    )
)

cleaned = (
    flattened
    .filter(F.col("created_at") >= F.lit("2025-03-01").cast("date"))
    .select(
        "created_at",
        "coupon_codes",
        "product_id",
        "price",
        F.array_join("coupon_codes", "|").alias("coupon_codes_text"),
        F.greatest("price", "qty").alias("greatest_price_qty"),
    )
)

enriched = (
    cleaned
    .filter(F.col("greatest_price_qty") < 47)
    .select(
        "*",
        F.dense_rank().over(w_grp).alias("greatest_price_qty_dense_rank"),
        F.count("greatest_price_qty").over(w2).alias("greatest_price_qty_count_2h"),
    )
)

metrics = (
    enriched
    .select(
        "created_at",
        "product_id",
        "price",
        "coupon_codes_text",
        "greatest_price_qty",
        "greatest_price_qty_count_2h",
        F.max("greatest_price_qty_count_2h").over(w3).alias("greatest_price_qty_count_2h_max_30m"),
        F.cume_dist().over(w4).alias("greatest_price_qty_count_2h_cume_dist"),
        F.count("greatest_price_qty_dense_rank").over(w2).alias("greatest_price_qty_dense_rank_count_2h"),
    )
)

enriched_carts = (
    metrics
    .select(
        "*",
        F.cume_dist().over(w5).alias("greatest_price_qty_cume_dist"),
        F.cume_dist().over(w6).alias("greatest_price_qty_count_2h_max_30m_cume_dist"),
        F.rank().over(w7).alias("greatest_price_qty_count_2h_max_30m_rank"),
        F.max("greatest_price_qty_count_2h_max_30m").over(w3).alias("greatest_price_qty_count_2h_max_30m_max_30m"),
    )
)

result = (
    enriched_carts
    .groupBy("product_id")
    .agg(
        F.avg("product_id").alias("avg_product_id"),
        F.max("created_at").alias("last_created_at"),
        F.count("*").alias("num_rows"),
        F.sum("greatest_price_qty_cume_dist").alias("total_greatest_price_qty_cume_dist"),
    )
    .orderBy(F.col("num_rows").desc())
)
