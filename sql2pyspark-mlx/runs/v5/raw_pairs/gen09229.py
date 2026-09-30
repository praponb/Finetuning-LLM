from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").asc_nulls_last(), "id")
w2 = Window.partitionBy("category")
w3 = Window.partitionBy("brand")
w4 = Window.partitionBy("category").orderBy(F.col("cost").asc_nulls_first())

ranked = (
    df_products
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

kept = (
    ranked
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.order_id") == 38), "left_semi")
)

kept_keys = (
    kept
    .select("category")
    .intersect(
        df_products
        .filter(F.col("cost") > 270)
        .select("category")
    )
)

verified = (
    kept
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

not_flagged = (
    verified
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.quantity") >= 5), "left_semi")
)

with_windows = (
    not_flagged
    .select(
        "*",
        F.corr("price", "stock_qty").over(w2).alias("corr_price_stock_qty"),
        F.stddev_samp("cost").over(w2).alias("category_stddev_samp_cost"),
        F.stddev_samp("cost").over(w3).alias("brand_stddev_samp_cost"),
        F.cume_dist().over(w4).alias("cost_cume_dist"),
        F.min("cost").over(w3).alias("brand_min_cost"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("brand").isNotNull() & (F.col("corr_price_stock_qty") > 0.2))
    .withColumn("greatest_brand_stddev_samp_cost_latest_rank", F.greatest("brand_stddev_samp_cost", "latest_rank"))
)

cleaned_products = (
    cleaned
    .filter(F.col("cost") > 600)
    .withColumn("latest_rank_str", F.col("latest_rank").cast("string"))
)

stats = (
    cleaned_products
    .withColumn("cost_cume_dist_tier", F.when(F.col("cost_cume_dist") >= 0.81, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["brand", "cost_cume_dist_tier"], ["brand"]],
        "brand",
        "cost_cume_dist_tier",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("brand").alias("distinct_brands"),
        F.sum(F.when(F.col("brand") == "Stark", F.col("price"))).alias("stark_total_price"),
        F.stddev_samp("price").alias("price_stddev"),
    )
    .filter(F.col("price_stddev") > 0)
)

result = (
    stats
    .select(
        "brand",
        "cost_cume_dist_tier",
        "num_rows",
        "stark_total_price",
        F.when(F.col("brand").rlike("^(Umbrella|Globex)$"), "PRIORITY").otherwise("EXTENDED").alias("brand_zone"),
    )
    .orderBy(F.col("stark_total_price").desc_nulls_last())
)
