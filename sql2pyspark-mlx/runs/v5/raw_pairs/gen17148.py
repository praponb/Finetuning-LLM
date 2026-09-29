from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("product_id", "store_id")
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("store_id", "product_id")
w_ordered = Window.partitionBy("product_id")
w5 = Window.orderBy("revenue")
w6 = Window.orderBy(F.col("product_id_min_corr_revenue_units_2").asc_nulls_first())
w7 = Window.orderBy(F.col("revenue_share_2").desc_nulls_last())
w8 = Window.orderBy(F.col("revenue").desc())

kept_keys = (
    df_store_sales
    .select("store_id", "id")
    .subtract(
        df_store_sales
        .filter(F.col("revenue") > 3200)
        .select("store_id", "id")
    )
)

reconciled = (
    df_store_sales
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .filter(F.col("revenue").between(2100, 2200))
    .withColumn("corr_revenue_units", F.corr("revenue", "units").over(w_part))
    .withColumn("revenue_share", F.round((F.col("revenue") / F.sum("revenue").over(w_part)), 4))
    .withColumn("corr_revenue_units_2", F.corr("revenue", "units").over(w2))
)

with_windows = (
    enriched
    .select(
        "units",
        "revenue",
        "corr_revenue_units_2",
        F.round((F.col("revenue") / F.sum("revenue").over(w3)), 4).alias("revenue_share_2"),
        F.stddev_samp("corr_revenue_units_2").over(w_ordered).alias("product_id_stddev_samp_corr_revenue_units_2"),
        F.min("corr_revenue_units_2").over(w_ordered).alias("product_id_min_corr_revenue_units_2"),
    )
)

scored = (
    with_windows
    .withColumn("revenue_cume_dist", F.cume_dist().over(w5))
    .withColumn("product_id_min_corr_revenue_units_2_percent_rank", F.percent_rank().over(w6))
    .withColumn("revenue_share_2_dense_rank", F.dense_rank().over(w7))
)

cleaned = (
    scored
    .filter(F.col("revenue") <= 2300)
    .select(
        "revenue",
        "revenue_share_2",
        "product_id_min_corr_revenue_units_2",
        "product_id_min_corr_revenue_units_2_percent_rank",
        F.when(F.col("revenue_share_2") >= 0.67, "High").when(F.col("revenue_share_2") >= 0.38, "Medium").otherwise("Low").alias("revenue_share_2_tier"),
    )
)

scored_store_sales = cleaned.select("*", F.cume_dist().over(w8).alias("revenue_cume_dist"))

result = (
    scored_store_sales
    .select(
        "revenue",
        "product_id_min_corr_revenue_units_2",
        "product_id_min_corr_revenue_units_2_percent_rank",
        "revenue_share_2_tier",
        "revenue_cume_dist",
    )
    .orderBy(F.col("revenue").desc())
)
