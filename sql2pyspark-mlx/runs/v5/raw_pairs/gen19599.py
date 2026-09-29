from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("product_id", "store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("last_restock_period", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy("running_max_quantity", "id")
w_part = Window.partitionBy("product_id").orderBy("running_max_quantity_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("store_id").orderBy("running_count_running_count_running_count_quantity", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.partitionBy("store_id").orderBy("running_sum_running_count_quantity", "id").rowsBetween(-5, -1)
w8 = Window.partitionBy("store_id").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("running_avg_running_count_quantity", "id").rowsBetween(-3, Window.currentRow)
w10 = Window.partitionBy("store_id").orderBy("running_avg_running_count_quantity_avg_5_prior", "id").rowsBetween(-5, Window.currentRow)
w11 = Window.partitionBy("store_id").orderBy(F.col("running_avg_running_count_quantity_avg_5_prior").desc(), "id")
w12 = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-3, -1)
w13 = Window.partitionBy("store_id").orderBy("running_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w14 = Window.partitionBy("store_id")
w15 = Window.partitionBy("store_id").orderBy("sqft_avg_6_avg_3", "id")
w16 = Window.orderBy("running_avg_running_count_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-09-06").cast("date"))
    .withColumn("last_restock_period", F.date_trunc("YEAR", "last_restock"))
)

metrics = (
    valid
    .select(
        "*",
        F.sum("quantity").over(w_running).alias("running_sum_quantity"),
        F.max("quantity").over(w_running).alias("running_max_quantity"),
        F.count("quantity").over(w_running).alias("running_count_quantity"),
        F.avg("quantity").over(w2).alias("quantity_avg_5"),
    )
)

scored = (
    metrics
    .withColumn("running_min_running_count_quantity", F.min("running_count_quantity").over(w3))
    .withColumn("running_avg_running_count_quantity", F.avg("running_count_quantity").over(w3))
    .withColumn("running_sum_running_count_quantity", F.sum("running_count_quantity").over(w3))
    .withColumn("running_count_running_count_quantity", F.count("running_count_quantity").over(w3))
    .withColumn("running_max_quantity_bucket", F.ntile(3).over(w4))
)

with_windows = (
    scored
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        "last_restock_period",
        "running_avg_running_count_quantity",
        "running_sum_running_count_quantity",
        F.count("running_count_running_count_quantity").over(w_part).alias("running_count_running_count_running_count_quantity"),
        F.max("running_count_running_count_quantity").over(w_part).alias("running_max_running_count_running_count_quantity"),
        F.avg("running_count_running_count_quantity").over(w_part).alias("running_avg_running_count_running_count_quantity"),
    )
)

joined = (
    with_windows
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "inner")
    .select(
        "a.id",
        "a.store_id",
        "a.last_restock",
        "a.running_avg_running_count_quantity",
        "a.running_sum_running_count_quantity",
        "a.running_count_running_count_running_count_quantity",
        F.col("p.sqft"),
        F.col("p.store_name"),
    )
)

scored_inventory = (
    joined
    .filter(F.col("store_id").isin([1, 4]))
    .select(
        "id",
        "store_id",
        "last_restock",
        "running_avg_running_count_quantity",
        "running_sum_running_count_quantity",
        "sqft",
        "store_name",
        F.avg("sqft").over(w_grp).alias("sqft_avg_6"),
        F.avg("running_avg_running_count_quantity").over(w7).alias("running_avg_running_count_quantity_avg_5_prior"),
        F.sum("sqft").over(w8).alias("running_sqft"),
    )
)

scored_inventory_2 = (
    scored_inventory
    .select(
        "id",
        "store_id",
        "last_restock",
        "running_avg_running_count_quantity",
        "running_avg_running_count_quantity_avg_5_prior",
        "running_sqft",
        F.avg("sqft_avg_6").over(w9).alias("sqft_avg_6_avg_3"),
        F.avg("running_sqft").over(w10).alias("running_sqft_avg_5"),
        F.ntile(5).over(w11).alias("running_avg_running_count_quantity_avg_5_prior_bucket"),
    )
)

result = (
    scored_inventory_2
    .select(
        "id",
        "last_restock",
        "running_avg_running_count_quantity",
        "running_avg_running_count_quantity_avg_5_prior",
        "running_sqft_avg_5",
        "running_avg_running_count_quantity_avg_5_prior_bucket",
        F.avg("running_avg_running_count_quantity_avg_5_prior_bucket").over(w12).alias("running_avg_running_count_quantity_avg_5_prior_bucket_avg_3_prior"),
        F.sum("running_sqft_avg_5").over(w13).alias("cumulative_running_sqft_avg_5"),
        F.min("running_avg_running_count_quantity_avg_5_prior_bucket").over(w14).alias("store_id_min_running_avg_running_count_quantity_avg_5_prior_bucket"),
        F.row_number().over(w15).alias("seq_no"),
        F.last("running_sqft_avg_5", ignorenulls=True).over(w16).alias("last_known_running_sqft_avg_5"),
    )
)
