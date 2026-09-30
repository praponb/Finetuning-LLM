from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc_nulls_last(), "id")
w2 = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").desc(), "id")
w_part = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id")
w5 = Window.partitionBy("product_id").orderBy("unit_price", "id")
w6 = Window.orderBy("least_unit_price_share_running_unit_price", "id")
w7 = Window.orderBy("next_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("least_unit_price_share_running_unit_price").asc_nulls_last())
w9 = Window.orderBy(F.col("prev_unit_price_share").desc(), "id")

ranked = (
    df_order_items
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

latest = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "product_id", "unit_price")
)

enriched = (
    latest
    .select(
        "id",
        "unit_price",
        F.sum("unit_price").over(w_part).alias("running_unit_price"),
        F.avg("unit_price").over(w4).alias("product_id_avg_unit_price"),
        F.first("unit_price", ignorenulls=True).over(w_part).alias("first_known_unit_price"),
        F.lead("unit_price", 1).over(w5).alias("next_unit_price"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w4)), 4).alias("unit_price_share"),
    )
)

filtered = (
    enriched
    .filter(F.col("unit_price_share") < 0.42)
    .select(
        "id",
        "unit_price",
        "running_unit_price",
        "first_known_unit_price",
        "next_unit_price",
        "unit_price_share",
        (F.col("first_known_unit_price") + F.col("product_id_avg_unit_price")).alias("first_known_unit_price_plus_product_id_avg_unit_price"),
        (F.col("running_unit_price") * F.col("product_id_avg_unit_price")).alias("running_unit_price_times_product_id_avg_unit_price"),
    )
)

filtered_order_items = (
    filtered
    .filter(F.col("first_known_unit_price_plus_product_id_avg_unit_price") > 64)
    .withColumn("least_unit_price_share_running_unit_price", F.least("unit_price_share", "running_unit_price"))
)

with_windows = (
    filtered_order_items
    .filter(F.col("least_unit_price_share_running_unit_price") > 21)
    .select(
        "*",
        F.lag("first_known_unit_price").over(w6).alias("prev_first_known_unit_price"),
        F.lag("unit_price_share").over(w6).alias("prev_unit_price_share"),
        F.row_number().over(w6).alias("row_seq"),
        F.sum("next_unit_price").over(w7).alias("cumulative_next_unit_price"),
    )
)

enriched_order_items = (
    with_windows
    .select(
        "id",
        "running_unit_price",
        "next_unit_price",
        "unit_price_share",
        "first_known_unit_price_plus_product_id_avg_unit_price",
        "running_unit_price_times_product_id_avg_unit_price",
        "least_unit_price_share_running_unit_price",
        "prev_first_known_unit_price",
        "row_seq",
        "cumulative_next_unit_price",
        F.cume_dist().over(w8).alias("least_unit_price_share_running_unit_price_cume_dist"),
        F.ntile(10).over(w9).alias("prev_unit_price_share_bucket"),
    )
)

result = (
    enriched_order_items
    .select(
        "id",
        "first_known_unit_price_plus_product_id_avg_unit_price",
        "running_unit_price_times_product_id_avg_unit_price",
        "least_unit_price_share_running_unit_price",
        "prev_first_known_unit_price",
        "row_seq",
        "cumulative_next_unit_price",
        "least_unit_price_share_running_unit_price_cume_dist",
    )
    .orderBy("id")
    .limit(20)
)
