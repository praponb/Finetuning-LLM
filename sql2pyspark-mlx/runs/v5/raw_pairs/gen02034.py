from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc_nulls_last(), "id")
w_recent = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("product_id", "store_id")
w4 = Window.partitionBy("product_id").orderBy("quantity", "id")
w_hist = Window.partitionBy("region").orderBy("seq_no", "id")
w6 = Window.partitionBy("store_id")
w7 = Window.partitionBy("store_id").orderBy(F.col("row_seq").asc_nulls_last())
w8 = Window.partitionBy("store_id").orderBy("store_id_min_seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("store_id").orderBy("row_seq", "id")
w10 = Window.orderBy("row_seq_filled", "id")

latest = (
    df_inventory
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_windows = (
    latest
    .filter(F.col("quantity") > 189)
    .withColumn("quantity_avg_3_prior", F.avg("quantity").over(w_recent))
    .withColumn("product_id_count_quantity", F.count("quantity").over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
)

with_dims = (
    with_windows
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "inner")
    .select(
        "a.id",
        "a.store_id",
        "a.product_id_count_quantity",
        "a.seq_no",
        F.col("s.region"),
    )
)

scored = (
    with_dims
    .select(
        "id",
        "store_id",
        "product_id_count_quantity",
        F.lead("seq_no").over(w_hist).alias("next_seq_no"),
        F.lead("product_id_count_quantity").over(w_hist).alias("next_product_id_count_quantity"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.min("seq_no").over(w6).alias("store_id_min_seq_no"),
    )
)

base = (
    scored
    .filter(F.col("store_id_min_seq_no") > 6)
    .select("*", F.coalesce("row_seq", F.lit(0)).alias("row_seq_filled"))
)

result = (
    base
    .select(
        "id",
        "next_product_id_count_quantity",
        "store_id_min_seq_no",
        "row_seq_filled",
        F.rank().over(w7).alias("row_seq_rank"),
        F.first("product_id_count_quantity", ignorenulls=True).over(w8).alias("first_known_product_id_count_quantity"),
        F.lag("next_product_id_count_quantity", 1).over(w9).alias("prev_next_product_id_count_quantity"),
        F.min("row_seq_filled").over(w6).alias("store_id_min_row_seq_filled"),
        F.lead("next_seq_no", 2).over(w10).alias("next_next_seq_no"),
    )
)
