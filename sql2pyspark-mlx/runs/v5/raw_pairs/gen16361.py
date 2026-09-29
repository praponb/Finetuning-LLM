from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id")
w_part = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy(F.col("row_seq").desc_nulls_first())
w4 = Window.partitionBy("store_id").orderBy(F.col("launch_date").desc(), "id")
w5 = Window.partitionBy("brand").orderBy("launch_date", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("product_id").orderBy(F.col("sqft").desc(), "id")

valid = (
    df_inventory
    .filter((F.col("last_restock") > F.to_date(F.lit("2023-04-28"))) & (F.col("quantity") <= 98))
    .withColumn("last_restock_minus_29d", F.date_sub("last_restock", 29))
)

with_windows = (
    valid
    .withColumn("prev_quantity", F.lag("quantity").over(w_hist))
    .withColumn("row_seq", F.row_number().over(w_hist))
)

metrics = (
    with_windows
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "row_seq",
        F.sum("row_seq").over(w_part).alias("running_row_seq"),
        F.dense_rank().over(w_grp).alias("row_seq_dense_rank"),
    )
)

enriched = (
    metrics
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.last_restock",
        "s.row_seq",
        "s.running_row_seq",
        F.col("p.brand"),
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
    )
)

enriched_inventory = (
    enriched
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "inner")
    .filter(~F.col("d.region").isin(["North", "East"]))
    .select("e.*", F.col("d.opened_date"), F.col("d.sqft"), F.col("d.region"))
)

latest = (
    enriched_inventory
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

with_windows_inventory = (
    latest
    .select(
        "*",
        F.avg("running_row_seq").over(w5).alias("running_row_seq_avg_3"),
        F.ntile(3).over(w6).alias("sqft_bucket"),
    )
)

result = (
    with_windows_inventory
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "inner")
    .cube(F.col("d.brand").alias("product_brand"), F.col("s.region").alias("region"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("s.sqft")), 2).alias("avg_sqft"),
        F.sum(F.col("s.stock_qty")).alias("total_stock_qty"),
    )
)
