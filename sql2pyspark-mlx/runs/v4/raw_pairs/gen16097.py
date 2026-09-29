from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id")
w2 = Window.partitionBy("product_id").orderBy("quantity")
w3 = Window.partitionBy("store_id").orderBy(F.col("quantity").asc_nulls_first())
w4 = Window.partitionBy("product_id", "store_id").orderBy("quantity_rank", "id")
w5 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("store_id", "product_id").orderBy("cumulative_quantity_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("product_id").orderBy("quantity_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("store_id").orderBy("cumulative_quantity_rank", "id").rowsBetween(-3, -1)
w9 = Window.partitionBy("category", "store_id").orderBy(F.col("quantity").asc_nulls_last(), "id")

metrics = (
    df_inventory
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.percent_rank().over(w2).alias("quantity_percent_rank"),
        F.rank().over(w3).alias("quantity_rank"),
    )
)

with_windows = (
    metrics
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("cumulative_quantity_rank", F.sum("quantity_rank").over(w5))
)

with_windows_inventory = (
    with_windows
    .withColumn("running_quantity_percent_rank", F.sum("quantity_percent_rank").over(w6))
    .withColumn("last_known_seq_no", F.last("seq_no", ignorenulls=True).over(w7))
    .withColumn("seq_no_avg_3_prior", F.avg("seq_no").over(w8))
)

with_dims = (
    with_windows_inventory
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.category"), F.col("d.product_name"))
)

deduped = (
    with_dims
    .withColumn("rank_in_group", F.row_number().over(w9))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .select(
        "id",
        "product_id",
        "last_restock",
        "row_seq",
        "quantity_percent_rank",
        "quantity_rank",
        "seq_no",
        "cumulative_quantity_rank",
        "running_quantity_percent_rank",
    )
    .orderBy("product_id")
)
