from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("units_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("units_filled", "id")
w3 = Window.partitionBy("store_id").orderBy("units", "id")
w4 = Window.partitionBy("product_id")
w5 = Window.partitionBy("store_id")
w6 = Window.orderBy("units_filled")
w7 = Window.partitionBy("product_id", "store_id")

valid = (
    df_store_sales
    .filter(F.col("sale_date") >= F.to_date(F.lit("2023-08-17")))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        F.coalesce("units", F.lit(0)).alias("units_filled"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.sum("units").over(w).alias("running_units"),
        F.lead("units_filled", 2).over(w2).alias("next_units_filled"),
        F.lag("units_filled", 2).over(w3).alias("prev_units_filled"),
    )
)

matched_keys = (
    with_windows
    .select("product_id")
    .subtract(
        df_store_sales
        .filter(F.col("sale_date") >= "2024-04-01")
        .select("product_id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

with_windows_store_sales = (
    verified
    .withColumn("units_share", F.round((F.col("units") / F.sum("units").over(w4)), 4))
    .withColumn("store_id_stddev_samp_units", F.stddev_samp("units").over(w5))
    .withColumn("units_filled_cume_dist", F.cume_dist().over(w6))
    .withColumn("product_id_sum_running_units", F.sum("running_units").over(w7))
    .withColumn("units_share_2", F.round((F.col("units") / F.sum("units").over(w5)), 4))
)

with_dims = (
    with_windows_store_sales
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.stock_qty"), F.col("p.brand"))
)

valid_store_sales = (
    with_dims
    .filter(F.col("brand").isNotNull())
    .select("*", F.col("units_share").cast("int").alias("units_share_int"))
)

with_dims_store_sales = (
    valid_store_sales
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.product_name"), F.col("p.launch_date"))
)

result = (
    with_dims_store_sales
    .select(
        "store_id",
        "sale_date",
        "units_filled",
        "running_units",
        "prev_units_filled",
        "units_share",
        "units_filled_cume_dist",
        "product_id_sum_running_units",
        "units_share_2",
    )
)
