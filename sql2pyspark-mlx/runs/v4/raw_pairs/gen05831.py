from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("unit_price_int").desc())
w2 = Window.orderBy("unit_price_int", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.orderBy("unit_price", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.orderBy("quantity_avg_3", "id")
w5 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("unit_price_int_dense_rank", "id")
w7 = Window.orderBy("quantity_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("unit_price").desc(), "id")
w9 = Window.orderBy("first_known_unit_price", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_order_items
    .filter(F.col("unit_price") <= 360)
    .select(
        "id",
        "quantity",
        "unit_price",
        F.when(F.col("unit_price") >= 430, "High").when(F.col("unit_price") >= 70, "Medium").otherwise("Low").alias("unit_price_band"),
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.dense_rank().over(w).alias("unit_price_int_dense_rank"),
        F.avg("quantity").over(w2).alias("quantity_avg_3"),
        F.avg("unit_price").over(w3).alias("unit_price_avg_2"),
    )
)

valid = (
    metrics
    .filter(F.col("unit_price_int") == 41)
    .select(
        "*",
        (F.col("unit_price_int") * F.col("unit_price")).alias("unit_price_int_times_unit_price"),
        F.when(F.col("unit_price_int_dense_rank") >= 9, "High").when(F.col("unit_price_int_dense_rank") >= 5, "Medium").otherwise("Low").alias("unit_price_int_dense_rank_band"),
    )
)

enriched = (
    valid
    .select(
        "*",
        F.lead("unit_price", 1).over(w4).alias("next_unit_price"),
        F.first("unit_price", ignorenulls=True).over(w5).alias("first_known_unit_price"),
        F.lag("unit_price_int_times_unit_price", 1).over(w6).alias("prev_unit_price_int_times_unit_price"),
    )
)

base = (
    enriched
    .filter(F.col("quantity_avg_3") < 5)
    .select("*", F.round("unit_price", 1).alias("unit_price_rounded"))
)

filtered = (
    base
    .filter(F.col("next_unit_price") > 320)
    .select(
        "*",
        F.col("quantity").cast("string").alias("quantity_str"),
        (F.col("unit_price_rounded") + F.col("unit_price")).alias("unit_price_rounded_plus_unit_price"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.sum("unit_price").over(w7).alias("running_unit_price"),
        F.ntile(3).over(w8).alias("unit_price_bucket"),
        F.avg("unit_price_int_times_unit_price").over(w9).alias("unit_price_int_times_unit_price_avg_6"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("prev_unit_price_int_times_unit_price") >= 63, "PREMIUM").otherwise("STANDARD").alias("prev_unit_price_int_times_unit_price_tier"),
        F.when(F.col("unit_price_int") >= 89, "PREMIUM").otherwise("STANDARD").alias("unit_price_int_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.countDistinct("id").alias("distinct_ids_3"),
        F.percentile_approx("running_unit_price", 0.5).alias("running_unit_price_p50"),
    )
    .orderBy(F.col("distinct_ids_3").desc())
)
