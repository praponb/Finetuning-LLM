from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")

valid = (
    df_order_items
    .filter(F.col("unit_price") <= 200)
    .select(
        "id",
        "order_id",
        "quantity",
        F.greatest("quantity", "unit_price").alias("greatest_quantity_unit_price"),
    )
)

filtered = (
    valid
    .filter(F.col("order_id").isin([2, 6, 11]))
    .select(
        "*",
        F.floor("greatest_quantity_unit_price").alias("greatest_quantity_unit_price_whole"),
        F.when(F.col("greatest_quantity_unit_price") >= 64, "High").when(F.col("greatest_quantity_unit_price") >= 17, "Medium").otherwise("Low").alias("greatest_quantity_unit_price_band"),
    )
)

with_windows = (
    filtered
    .withColumn("order_id_avg_quantity", F.avg("quantity").over(w))
    .withColumn("order_id_sum_quantity", F.sum("quantity").over(w))
)

cleaned = (
    with_windows
    .filter(F.col("quantity").between(6, 8))
    .select(
        "id",
        "quantity",
        "greatest_quantity_unit_price",
        "greatest_quantity_unit_price_whole",
        "greatest_quantity_unit_price_band",
        "order_id_avg_quantity",
        "order_id_sum_quantity",
        F.round((F.col("order_id_avg_quantity") * 0.85), 2).alias("order_id_avg_quantity_scaled"),
    )
)

result = (
    cleaned
    .withColumn("greatest_quantity_unit_price_tier", F.when(F.col("greatest_quantity_unit_price") >= 85, "PREMIUM").otherwise("STANDARD"))
    .withColumn("order_id_avg_quantity_scaled_tier", F.when(F.col("order_id_avg_quantity_scaled") >= 75, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["greatest_quantity_unit_price_tier", "order_id_avg_quantity_scaled_tier"], ["greatest_quantity_unit_price_tier"]],
        "greatest_quantity_unit_price_tier",
        "order_id_avg_quantity_scaled_tier",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("greatest_quantity_unit_price_whole").alias("avg_greatest_quantity_unit_price_whole"),
        F.percentile_approx("order_id_avg_quantity_scaled", 0.9).alias("order_id_avg_quantity_scaled_p90"),
        F.percentile_approx("greatest_quantity_unit_price_whole", 0.5).alias("greatest_quantity_unit_price_whole_p50"),
    )
    .orderBy(F.col("order_id_avg_quantity_scaled_tier").desc())
)
