from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy(F.col("last_known_quantity").desc_nulls_first(), "id")
w4 = Window.partitionBy("store_id").orderBy("last_known_quantity_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id")
w6 = Window.orderBy(F.col("last_known_quantity_2").desc_nulls_first())

filtered = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-04-26").cast("date"))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.date_trunc("year", "last_restock").alias("last_restock_period"),
    )
)

with_windows = (
    filtered
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w))
    .withColumn("last_known_quantity_2", F.last("quantity", ignorenulls=True).over(w2))
)

valid = (
    with_windows
    .filter(F.col("last_restock") >= F.lit("2023-08-22").cast("date"))
    .select("*", F.floor("last_known_quantity").alias("last_known_quantity_whole"))
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "store_id", "last_known_quantity", "last_known_quantity_2")
)

result = (
    latest
    .select(
        "id",
        "store_id",
        F.sum("last_known_quantity_2").over(w4).alias("running_last_known_quantity_2"),
        F.stddev_samp("last_known_quantity_2").over(w5).alias("store_id_stddev_samp_last_known_quantity_2"),
        F.percent_rank().over(w6).alias("last_known_quantity_2_percent_rank"),
    )
)
