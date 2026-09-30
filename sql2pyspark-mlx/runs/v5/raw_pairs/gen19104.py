from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w3 = Window.partitionBy("store_id")
w4 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy("quantity_bucket_rounded", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.partitionBy("product_id").orderBy("quantity_avg_2", "id").rowsBetween(-3, -1)
w7 = Window.partitionBy("product_id").orderBy("store_id_sum_quantity_rounded", "id")
w8 = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("product_id").orderBy(F.col("avg_quantity_avg_2_avg_3_prior").desc())

enriched = (
    df_inventory
    .filter(F.col("product_id").isin([1, 3, 7, 10]))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.avg("quantity").over(w).alias("quantity_avg_2"),
        F.ntile(3).over(w2).alias("quantity_bucket"),
        F.min("quantity").over(w3).alias("store_id_min_quantity"),
        F.sum("quantity").over(w3).alias("store_id_sum_quantity"),
    )
)

cleaned = (
    enriched
    .filter((F.col("last_restock") >= F.lit("2023-03-17").cast("date")) & (F.col("quantity_avg_2") >= 219))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "quantity_avg_2",
        F.round("store_id_sum_quantity", 0).alias("store_id_sum_quantity_rounded"),
        F.round("quantity_bucket", 0).alias("quantity_bucket_rounded"),
    )
)

enriched_inventory = (
    cleaned
    .withColumn("quantity_avg_3", F.avg("quantity").over(w4))
    .withColumn("quantity_avg_2_avg_2", F.avg("quantity_avg_2").over(w5))
    .withColumn("quantity_avg_2_avg_3_prior", F.avg("quantity_avg_2").over(w6))
    .withColumn("store_id_sum_quantity_rounded_bucket", F.ntile(10).over(w7))
    .withColumn("running_quantity_avg_2", F.sum("quantity_avg_2").over(w8))
)

joined = (
    enriched_inventory
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.product_id",
        "s.quantity",
        "s.last_restock",
        "s.quantity_avg_2",
        "s.store_id_sum_quantity_rounded",
        "s.quantity_bucket_rounded",
        "s.quantity_avg_2_avg_2",
        "s.quantity_avg_2_avg_3_prior",
        "s.running_quantity_avg_2",
        F.col("d.stock_qty"),
    )
)

valid = (
    joined
    .filter(F.col("quantity_avg_2_avg_3_prior") <= 200)
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity_avg_2",
        "store_id_sum_quantity_rounded",
        "quantity_bucket_rounded",
        "quantity_avg_2_avg_3_prior",
        "stock_qty",
        (F.col("running_quantity_avg_2") * F.col("quantity_avg_2_avg_2")).alias("running_quantity_avg_2_times_quantity_avg_2_avg_2"),
    )
)

kept_keys = (
    valid
    .select("id")
    .intersect(
        df_inventory
        .filter(F.col("store_id").isin([2, 5, 6]))
        .select("id")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        "product_id",
        F.date_trunc("MONTH", "last_restock").alias("activity_month"),
    )
    .agg(
        F.round(F.avg("quantity_avg_2_avg_3_prior"), 2).alias("avg_quantity_avg_2_avg_3_prior"),
        F.round(F.avg("running_quantity_avg_2_times_quantity_avg_2_avg_2"), 2).alias("avg_running_quantity_avg_2_times_quantity_avg_2_avg_2"),
    )
    .filter(F.col("avg_quantity_avg_2_avg_3_prior") < 1000000)
    .withColumn("avg_quantity_avg_2_avg_3_prior_dense_rank", F.dense_rank().over(w9))
)
