from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("last_restock").asc_nulls_first())
w2 = Window.partitionBy("product_id").orderBy("quantity_minus_dedup_rank", "id")
w3 = Window.partitionBy("store_id").orderBy("last_restock", "id")
w4 = Window.orderBy("next_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id", "store_id").orderBy("prev_quantity_minus_dedup_rank", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("product_id").orderBy("next_quantity", "id")
w7 = Window.partitionBy("product_id").orderBy(F.col("quantity_minus_dedup_rank").desc(), "id")
w8 = Window.orderBy("last_restock", "id")
w9 = Window.orderBy("quantity_minus_dedup_rank_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("product_id").orderBy(F.col("next_quantity_minus_dedup_rank").desc(), "id")
w11 = Window.partitionBy("product_id").orderBy("next_quantity_minus_dedup_rank", "id")

deduped = (
    df_inventory
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

cleaned = (
    deduped
    .filter(F.col("last_restock").between("2023-09-01", "2023-10-01"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "dedup_rank",
        (F.col("quantity") - F.col("dedup_rank")).alias("quantity_minus_dedup_rank"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "quantity_minus_dedup_rank",
        F.lag("quantity_minus_dedup_rank").over(w2).alias("prev_quantity_minus_dedup_rank"),
        F.lead("quantity").over(w2).alias("next_quantity"),
        F.row_number().over(w2).alias("event_seq"),
        F.lead("quantity_minus_dedup_rank", 1).over(w3).alias("next_quantity_minus_dedup_rank"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "quantity_minus_dedup_rank",
        "prev_quantity_minus_dedup_rank",
        "event_seq",
        "next_quantity_minus_dedup_rank",
        F.sum("quantity").over(w4).alias("cumulative_quantity"),
        F.avg("quantity_minus_dedup_rank").over(w5).alias("quantity_minus_dedup_rank_avg_3"),
        F.lead("quantity_minus_dedup_rank", 2).over(w6).alias("next_quantity_minus_dedup_rank_2"),
        F.ntile(3).over(w7).alias("quantity_minus_dedup_rank_bucket"),
        F.row_number().over(w8).alias("event_seq_2"),
    )
)

base = (
    metrics
    .filter(F.col("store_id") == 1)
    .select(
        "id",
        "product_id",
        "quantity_minus_dedup_rank",
        "prev_quantity_minus_dedup_rank",
        "next_quantity_minus_dedup_rank",
        "cumulative_quantity",
        "quantity_minus_dedup_rank_avg_3",
        "next_quantity_minus_dedup_rank_2",
        "quantity_minus_dedup_rank_bucket",
        (F.col("event_seq_2") + F.col("next_quantity_minus_dedup_rank_2")).alias("event_seq_2_plus_next_quantity_minus_dedup_rank_2"),
    )
)

base_inventory = (
    base
    .filter(F.col("next_quantity_minus_dedup_rank_2").isNotNull())
    .select(
        "id",
        "product_id",
        "quantity_minus_dedup_rank",
        "prev_quantity_minus_dedup_rank",
        "next_quantity_minus_dedup_rank",
        "cumulative_quantity",
        "quantity_minus_dedup_rank_bucket",
        F.round((F.col("next_quantity_minus_dedup_rank_2") * 1.1), 2).alias("adjusted_next_quantity_minus_dedup_rank_2"),
    )
)

result = (
    base_inventory
    .select(
        "id",
        "product_id",
        "cumulative_quantity",
        "quantity_minus_dedup_rank_bucket",
        F.first("prev_quantity_minus_dedup_rank", ignorenulls=True).over(w9).alias("first_known_prev_quantity_minus_dedup_rank"),
        F.row_number().over(w10).alias("seq_no"),
        F.row_number().over(w11).alias("row_seq"),
    )
)
