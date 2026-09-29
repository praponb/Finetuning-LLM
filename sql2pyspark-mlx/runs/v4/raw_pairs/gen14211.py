from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")
w2 = Window.partitionBy("product_id")
w3 = Window.orderBy("unit_price_filled", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("product_id").orderBy("quantity", "id")
w5 = Window.partitionBy("product_id").orderBy(F.col("unit_price_filled_2").desc_nulls_last())
w_recent = Window.partitionBy("order_id").orderBy("unit_price_filled_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("order_id")

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
    )
)

valid = (
    base
    .filter(F.col("product_id").isin([12, 21, 24]))
    .withColumn("unit_price_filled_2", F.coalesce("unit_price", F.lit(0)))
)

enriched = (
    valid
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.corr("unit_price", "quantity").over(w2).alias("corr_unit_price_quantity"),
        F.avg("quantity").over(w3).alias("quantity_avg_2_prior"),
        F.lag("unit_price", 2).over(w4).alias("prev_unit_price"),
        F.cume_dist().over(w5).alias("unit_price_filled_2_cume_dist"),
    )
)

metrics = (
    enriched
    .withColumn("last_known_unit_price_filled_2", F.last("unit_price_filled_2", ignorenulls=True).over(w_recent))
    .withColumn("order_id_avg_quantity", F.avg("quantity").over(w7))
)

matched_keys = (
    metrics
    .select("id", "order_id")
    .intersect(
        df_order_items
        .filter(F.col("product_id").isNull())
        .select("id", "order_id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("quantity_avg_2_prior") >= 9, "HIGH").otherwise("LOW").alias("quantity_avg_2_prior_tier"),
        "product_id",
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.percentile_approx("last_known_unit_price_filled_2", [0.25, 0.5, 0.75]).alias("last_known_unit_price_filled_2_quartiles"),
    )
    .filter(F.col("total_quantity") > 0)
    .orderBy(F.col("product_id").desc())
)
