from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")
w2 = Window.partitionBy("category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("category", "brand").orderBy("cost", "id")
w4 = Window.partitionBy("supplier_id").orderBy("stock_qty", "id")
w5 = Window.partitionBy("category")
w6 = Window.partitionBy("category", "supplier_id")
w7 = Window.partitionBy("brand").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("supplier_id").orderBy(F.col("cost_bucket").desc_nulls_first())

scored = (
    df_products
    .select(
        "*",
        F.count("cost").over(w).alias("brand_count_cost"),
        F.first("cost", ignorenulls=True).over(w2).alias("first_known_cost"),
        F.ntile(5).over(w3).alias("cost_bucket"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.ntile(4).over(w4).alias("stock_qty_bucket"),
        F.stddev_samp("brand_count_cost").over(w5).alias("category_stddev_samp_brand_count_cost"),
        F.round((F.col("brand_count_cost") / F.sum("brand_count_cost").over(w6)), 4).alias("brand_count_cost_share"),
        F.first("cost_bucket", ignorenulls=True).over(w7).alias("first_known_cost_bucket"),
        F.corr("price", "cost").over(w).alias("corr_price_cost"),
    )
)

matched_keys = (
    metrics
    .select("brand", "category")
    .exceptAll(
        df_products
        .filter(F.col("supplier_id").isNotNull())
        .select("brand", "category")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select(
        "*",
        F.avg("category_stddev_samp_brand_count_cost").over(w5).alias("category_avg_category_stddev_samp_brand_count_cost"),
        F.rank().over(w8).alias("cost_bucket_rank"),
    )
)

kept_keys = (
    enriched
    .select("brand", "supplier_id")
    .intersect(
        df_products
        .filter(F.col("brand").isin(["Initech", "Globex"]))
        .select("brand", "supplier_id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

daily = (
    verified
    .groupBy(F.date_trunc("WEEK", "launch_date").alias("week_start"))
    .agg(
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.round(F.sum("cost_bucket_rank") / F.nullif(F.count(F.when(F.col("category") == "Books", 1)), F.lit(0)), 2).alias("cost_bucket_rank_per_books"),
        F.avg("cost_bucket").alias("avg_cost_bucket"),
        F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    daily
    .select("week_start", "cost_bucket_rank_per_books", "acme_count")
    .orderBy(F.col("acme_count").desc())
)
