from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc_nulls_last())
w3 = Window.partitionBy("product_id").orderBy("quantity", "id")
w_hist = Window.orderBy("running_max_unit_price", "id")
w5 = Window.partitionBy("product_id").orderBy("running_min_unit_price", "id")
w_grp = Window.partitionBy("product_id")
w7 = Window.partitionBy("product_id").orderBy("unit_price_percent_rank", "id")
w8 = Window.partitionBy("product_id").orderBy(F.col("running_min_unit_price").asc_nulls_first(), "id")
w9 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w10 = Window.partitionBy("next_unit_price_percent_rank_tier").orderBy(F.col("cnt").desc())

enriched = (
    df_order_items
    .select(
        "id",
        "product_id",
        "quantity",
        F.min("unit_price").over(w).alias("running_min_unit_price"),
        F.count("unit_price").over(w).alias("running_count_unit_price"),
        F.max("unit_price").over(w).alias("running_max_unit_price"),
        F.percent_rank().over(w2).alias("unit_price_percent_rank"),
    )
)

scored = (
    enriched
    .withColumn("next_unit_price_percent_rank", F.lead("unit_price_percent_rank").over(w3))
    .withColumn("next_running_count_unit_price", F.lead("running_count_unit_price").over(w3))
    .withColumn("prev_running_count_unit_price", F.lag("running_count_unit_price").over(w3))
    .withColumn("seq_no", F.row_number().over(w3))
)

enriched_order_items = (
    scored
    .withColumn("prev_unit_price_percent_rank", F.lag("unit_price_percent_rank", 2).over(w_hist))
    .withColumn("event_seq", F.row_number().over(w5))
    .withColumn("product_id_max_prev_running_count_unit_price", F.max("prev_running_count_unit_price").over(w_grp))
    .withColumn("seq_no_2", F.row_number().over(w7))
    .withColumn("product_id_min_seq_no", F.min("seq_no").over(w_grp))
)

ranked = (
    enriched_order_items
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
)

latest = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w9))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "product_id",
        "running_count_unit_price",
        "next_unit_price_percent_rank",
        "next_running_count_unit_price",
        "prev_unit_price_percent_rank",
        "event_seq",
        "product_id_max_prev_running_count_unit_price",
        "rn",
    )
)

result = (
    latest
    .groupBy(
        F.when(F.col("next_unit_price_percent_rank") >= 0.67, "HIGH").otherwise("LOW").alias("next_unit_price_percent_rank_tier"),
        F.when(F.col("next_running_count_unit_price") >= 15, "PREMIUM").otherwise("STANDARD").alias("next_running_count_unit_price_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("prev_unit_price_percent_rank"), 2).alias("avg_prev_unit_price_percent_rank"),
    )
    .withColumn("cnt_dense_rank", F.dense_rank().over(w10))
    .orderBy(F.col("next_unit_price_percent_rank_tier").desc_nulls_last())
)
