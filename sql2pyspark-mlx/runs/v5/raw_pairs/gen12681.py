from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity_filled").desc_nulls_first())
w2 = Window.partitionBy("store_id").orderBy(F.col("latest_rank_whole").desc_nulls_last())
w3 = Window.orderBy("latest_rank", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("store_id").orderBy("latest_rank_whole", "id")
w5 = Window.partitionBy("store_id").orderBy("quantity_whole_per_quantity_filled")
w6 = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.partitionBy("store_id").orderBy("next_quantity_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("least_quantity_filled_quantity_whole_per_quantity_filled", "id")
w9 = Window.partitionBy("store_id")
w10 = Window.partitionBy("store_id").orderBy("last_restock", "id")

base = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-05-21").cast("date")) & F.col("product_id").isin([6, 7, 8, 9]))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.ceil("quantity").alias("quantity_whole"),
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

base_inventory = (
    base
    .filter(F.col("quantity_whole") <= 50)
    .select(
        "id",
        "store_id",
        "last_restock",
        "quantity_whole",
        "quantity_filled",
        (F.col("quantity_whole") / F.nullif("quantity_filled", F.lit(0))).alias("quantity_whole_per_quantity_filled"),
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_label"),
    )
)

latest = (
    base_inventory
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

cleaned = (
    latest
    .filter((F.col("last_restock") > F.to_date(F.lit("2023-03-31"))) & (F.col("quantity_whole_per_quantity_filled") >= 53))
    .select(
        "id",
        "store_id",
        "last_restock",
        "quantity_filled",
        "quantity_whole_per_quantity_filled",
        "last_restock_label",
        "latest_rank",
        F.least("quantity_filled", "quantity_whole_per_quantity_filled").alias("least_quantity_filled_quantity_whole_per_quantity_filled"),
    )
)

base_inventory_2 = (
    cleaned
    .filter(F.col("quantity_whole_per_quantity_filled") > 76)
    .select("*", F.ceil("latest_rank").alias("latest_rank_whole"))
)

metrics = (
    base_inventory_2
    .filter(F.col("store_id").isin([1, 2, 5, 6]))
    .withColumn("latest_rank_whole_dense_rank", F.dense_rank().over(w2))
    .withColumn("quantity_filled_avg_2_prior", F.avg("quantity_filled").over(w3))
    .withColumn("next_quantity_filled", F.lead("quantity_filled", 2).over(w4))
    .withColumn("quantity_whole_per_quantity_filled_percent_rank", F.percent_rank().over(w5))
)

scored = (
    metrics
    .select(
        "id",
        "quantity_filled",
        "quantity_whole_per_quantity_filled",
        "latest_rank_whole",
        "next_quantity_filled",
        "quantity_whole_per_quantity_filled_percent_rank",
        F.avg("latest_rank_whole").over(w6).alias("latest_rank_whole_avg_6"),
        F.last("latest_rank_whole", ignorenulls=True).over(w7).alias("last_known_latest_rank_whole"),
        F.row_number().over(w8).alias("seq_no"),
        F.avg("next_quantity_filled").over(w9).alias("store_id_avg_next_quantity_filled"),
        F.lag("next_quantity_filled", 2).over(w10).alias("prev_next_quantity_filled"),
    )
)

result = (
    scored
    .select(
        "id",
        "quantity_filled",
        "quantity_whole_per_quantity_filled",
        "latest_rank_whole",
        "quantity_whole_per_quantity_filled_percent_rank",
        "latest_rank_whole_avg_6",
        "last_known_latest_rank_whole",
        "store_id_avg_next_quantity_filled",
    )
    .orderBy("quantity_filled", "store_id_avg_next_quantity_filled")
)
