from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity_whole", "id")
w_part = Window.partitionBy("product_id")
w_recent = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy("quantity", "id")
w5 = Window.partitionBy("product_id").orderBy("next_quantity", "id")
w6 = Window.partitionBy("store_id").orderBy(F.col("quantity_whole_avg_2").desc(), "id")
w7 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("product_id", "store_id").orderBy("quantity_whole_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("store_id").orderBy("cumulative_next_quantity", "id")
w10 = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").desc(), "id")
w11 = Window.partitionBy("store_id").orderBy("quantity_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("store_id")

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-05-29").cast("date"))
    .withColumn("quantity_whole", F.floor("quantity"))
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

metrics = (
    latest
    .withColumn("product_id_min_quantity_whole", F.min("quantity_whole").over(w_part))
    .withColumn("quantity_whole_avg_2", F.avg("quantity_whole").over(w_recent))
    .withColumn("next_quantity", F.lead("quantity", 1, 0).over(w4))
)

latest_inventory = (
    metrics
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

enriched = (
    latest_inventory
    .filter(F.col("store_id") == 4)
    .select(
        "*",
        F.ntile(5).over(w6).alias("quantity_whole_avg_2_bucket"),
        F.sum("next_quantity").over(w7).alias("cumulative_next_quantity"),
        F.last("quantity_whole_avg_2", ignorenulls=True).over(w8).alias("last_known_quantity_whole_avg_2"),
    )
)

result = (
    enriched
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        "quantity_whole",
        "product_id_min_quantity_whole",
        "quantity_whole_avg_2",
        "next_quantity",
        "quantity_whole_avg_2_bucket",
        "cumulative_next_quantity",
        F.row_number().over(w9).alias("row_seq"),
        F.row_number().over(w10).alias("seq_no"),
        F.last("last_known_quantity_whole_avg_2", ignorenulls=True).over(w11).alias("last_known_last_known_quantity_whole_avg_2"),
        F.stddev_samp("quantity_whole_avg_2").over(w12).alias("store_id_stddev_samp_quantity_whole_avg_2"),
        F.first("quantity_whole_avg_2", ignorenulls=True).over(w11).alias("first_known_quantity_whole_avg_2"),
    )
)
