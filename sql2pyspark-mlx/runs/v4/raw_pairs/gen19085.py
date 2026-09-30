from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_restock").desc_nulls_first(), "id")
w_grp = Window.partitionBy("product_id").orderBy("quantity", "id")
w_recent = Window.orderBy(F.col("quantity").desc(), "id")
w_hist = Window.orderBy("rn", "id")
w5 = Window.partitionBy("store_id").orderBy(F.col("quantity").asc_nulls_first())
w6 = Window.partitionBy("store_id").orderBy("rn")
w7 = Window.partitionBy("store_id").orderBy("next_quantity", "id")
w8 = Window.partitionBy("product_id", "store_id")
w9 = Window.partitionBy("product_id").orderBy("next_quantity", "id").rowsBetween(-5, Window.currentRow)
w10 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").asc_nulls_first())
w12 = Window.partitionBy("store_id").orderBy("next_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.partitionBy("product_id").orderBy(F.col("quantity_percent_rank_2").desc(), "id")
w14 = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity_percent_rank").desc_nulls_last())
w_running = Window.partitionBy("store_id").orderBy("rn", "id")
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.min("units").alias("min_store_sale_units"))

deduped = (
    df_inventory
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

metrics = (
    deduped
    .select(
        "*",
        F.lag("quantity", 1, 0).over(w_grp).alias("prev_quantity"),
        F.ntile(3).over(w_recent).alias("quantity_bucket"),
        F.lead("quantity", 1).over(w_hist).alias("next_quantity"),
        F.percent_rank().over(w5).alias("quantity_percent_rank"),
    )
)

deduped_inventory = (
    metrics
    .withColumn("latest_rank", F.rank().over(w6))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

deduped_inventory_2 = (
    deduped_inventory
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

scored = (
    deduped_inventory_2
    .withColumn("quantity_bucket_share", F.round((F.col("quantity_bucket") / F.sum("quantity_bucket").over(w8)), 4))
    .withColumn("quantity_bucket_avg_5", F.avg("quantity_bucket").over(w9))
    .withColumn("cumulative_quantity_bucket", F.sum("quantity_bucket").over(w10))
    .withColumn("quantity_percent_rank_2", F.percent_rank().over(w11))
)

enriched = (
    scored
    .withColumn("first_known_quantity_bucket", F.first("quantity_bucket", ignorenulls=True).over(w12))
    .withColumn("quantity_percent_rank_2_bucket", F.ntile(3).over(w13))
    .withColumn("quantity_percent_rank_percent_rank", F.percent_rank().over(w14))
    .withColumn("prev_quantity_bucket_share", F.lag("quantity_bucket_share", 2).over(w_running))
)

report = (
    enriched
    .select(
        "id",
        "product_id",
        "quantity",
        "rn",
        "prev_quantity",
        "quantity_percent_rank",
        "quantity_percent_rank_2",
        "first_known_quantity_bucket",
        "quantity_percent_rank_2_bucket",
    )
)

result = (
    report
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "quantity",
        "rn",
        "prev_quantity",
        "quantity_percent_rank",
        "quantity_percent_rank_2",
        "first_known_quantity_bucket",
        "quantity_percent_rank_2_bucket",
        F.col("min_store_sale_units"),
    )
)
