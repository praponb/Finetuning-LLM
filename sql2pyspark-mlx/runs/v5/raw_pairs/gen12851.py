from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("unit_price_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_order_items
    .filter(F.col("unit_price").between(180, 230))
    .select("id", "quantity", "unit_price", F.ceil("quantity").alias("quantity_whole"))
)

with_windows = (
    cleaned
    .select(
        "id",
        "quantity",
        "unit_price",
        "quantity_whole",
        F.min("quantity_whole").over(w).alias("running_min_quantity_whole"),
        F.count("quantity_whole").over(w).alias("running_count_quantity_whole"),
        F.max("quantity_whole").over(w).alias("running_max_quantity_whole"),
        F.avg("quantity_whole").over(w).alias("running_avg_quantity_whole"),
    )
)

filtered = (
    with_windows
    .filter(F.col("quantity") >= 6)
    .select(
        "id",
        "quantity",
        "unit_price",
        "quantity_whole",
        "running_min_quantity_whole",
        "running_count_quantity_whole",
        "running_max_quantity_whole",
        "running_avg_quantity_whole",
        F.col("running_avg_quantity_whole").cast("int").alias("running_avg_quantity_whole_int"),
        F.col("running_avg_quantity_whole").cast("int").alias("running_avg_quantity_whole_int_2"),
    )
)

cleaned_order_items = (
    filtered
    .filter(F.col("running_avg_quantity_whole_int") > 37)
    .select(
        "id",
        "quantity",
        "unit_price",
        "running_count_quantity_whole",
        "running_max_quantity_whole",
        "running_avg_quantity_whole_int",
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
    )
)

scored = (
    cleaned_order_items
    .select(
        "id",
        "quantity",
        "running_count_quantity_whole",
        "running_max_quantity_whole",
        "running_avg_quantity_whole_int",
        "unit_price_filled",
        F.last("running_count_quantity_whole", ignorenulls=True).over(w2).alias("last_known_running_count_quantity_whole"),
        F.first("unit_price_filled", ignorenulls=True).over(w2).alias("first_known_unit_price_filled"),
    )
)

base = (
    scored
    .filter(F.col("running_avg_quantity_whole_int") == 54)
    .select(
        "*",
        (F.col("first_known_unit_price_filled") - F.col("quantity")).alias("first_known_unit_price_filled_minus_quantity"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("last_known_running_count_quantity_whole") >= 18, "PREMIUM").otherwise("STANDARD").alias("last_known_running_count_quantity_whole_tier"),
        F.when(F.col("quantity") >= 7, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.round(F.avg("unit_price_filled"), 2).alias("avg_unit_price_filled"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("running_count_quantity_whole"), 2).alias("avg_running_count_quantity_whole"),
        F.sum("running_count_quantity_whole").alias("total_running_count_quantity_whole"),
        F.sum("first_known_unit_price_filled").alias("total_first_known_unit_price_filled"),
    )
    .orderBy(F.col("last_known_running_count_quantity_whole_tier").desc())
)
