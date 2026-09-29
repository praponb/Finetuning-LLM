from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("last_restock").desc_nulls_last(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity_plus_latest_rank").desc_nulls_last())
w3 = Window.partitionBy("product_id").orderBy(F.col("quantity_plus_latest_rank").desc_nulls_first(), "id")
w4 = Window.orderBy("quantity_filled", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.orderBy("quantity", "id")
w6 = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity_filled").desc(), "id")
w7 = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_last())
w8 = Window.partitionBy("store_id").orderBy(F.col("quantity_filled").desc_nulls_first())
w9 = Window.orderBy(F.col("quantity_dense_rank").desc(), "id")
w10 = Window.partitionBy("product_id").orderBy("latest_rank_2", "id")
w11 = Window.partitionBy("product_id")
w12 = Window.partitionBy("product_id", "store_id")

ranked = (
    df_inventory
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

base = (
    ranked
    .filter(F.col("store_id") == 6)
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "latest_rank",
        (F.col("quantity") + F.col("latest_rank")).alias("quantity_plus_latest_rank"),
    )
)

ranked_inventory = (
    base
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

valid = (
    ranked_inventory
    .filter((F.col("last_restock") >= F.lit("2023-03-22").cast("date")) & F.col("product_id").isin([3, 4, 5, 7]))
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
)

deduped = (
    valid
    .withColumn("latest_rank_2", F.row_number().over(w3))
    .filter(F.col("latest_rank_2") == 1)
)

metrics = (
    deduped
    .select(
        "*",
        F.avg("quantity_filled").over(w4).alias("quantity_filled_avg_3"),
        F.row_number().over(w5).alias("seq_no"),
        F.row_number().over(w6).alias("event_seq"),
        F.dense_rank().over(w7).alias("quantity_dense_rank"),
        F.rank().over(w8).alias("quantity_filled_rank"),
    )
)

metrics_inventory = (
    metrics
    .select(
        "*",
        F.ntile(5).over(w9).alias("quantity_dense_rank_bucket"),
        F.row_number().over(w10).alias("seq_no_2"),
        F.round((F.col("seq_no") / F.sum("seq_no").over(w11)), 4).alias("seq_no_share"),
        F.round((F.col("quantity_filled_rank") / F.sum("quantity_filled_rank").over(w12)), 4).alias("quantity_filled_rank_share"),
    )
)

daily = (
    metrics_inventory
    .withColumn("quantity_plus_latest_rank_tier", F.when(F.col("quantity_plus_latest_rank") >= 61, "LARGE").otherwise("SMALL"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "last_restock"))
    .groupingSets(
        [["quantity_plus_latest_rank_tier", "seq_no", "quarter_start"], ["quantity_plus_latest_rank_tier", "seq_no"]],
        "quantity_plus_latest_rank_tier",
        "seq_no",
        "quarter_start",
    )
    .agg(
        F.round(F.avg("quantity_filled_avg_3"), 2).alias("avg_quantity_filled_avg_3"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.bit_and("quantity").alias("quantity_bit_and_2"),
    )
)

result = (
    daily
    .select(
        "quantity_plus_latest_rank_tier",
        "seq_no",
        "quarter_start",
        "avg_quantity_filled_avg_3",
        "quantity_bit_and",
        "quantity_bit_and_2",
    )
)
