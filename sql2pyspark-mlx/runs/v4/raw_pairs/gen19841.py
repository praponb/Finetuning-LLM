from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity_p50").desc())

matched_keys = (
    df_inventory
    .select("store_id")
    .intersectAll(
        df_inventory
        .filter(F.col("product_id").isin([1, 6, 8, 9]))
        .select("store_id")
    )
)

verified = (
    df_inventory
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        "store_id",
        F.when(F.col("quantity") >= 247, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.max("quantity").alias("max_quantity"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
    )
    .filter(F.col("avg_quantity") >= 0)
    .withColumn("quantity_p50_percent_rank", F.percent_rank().over(w))
    .orderBy(F.col("store_id").desc())
)
