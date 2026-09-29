from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("store_id").orderBy("last_restock", "id")
w3 = Window.partitionBy("store_id").orderBy("next_quantity_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("next_quantity_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy(F.col("running_seq_no").desc(), "id")
w6 = Window.partitionBy("store_id").orderBy(F.col("running_min_seq_no").desc(), "id")
w_part = Window.partitionBy("store_id").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part_2 = Window.partitionBy("store_id")
w_grp = Window.partitionBy("store_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("store_id").orderBy(F.col("running_sum_seq_no").asc_nulls_first())

enriched = (
    df_inventory
    .select(
        "id",
        "store_id",
        "quantity",
        F.lead("quantity").over(w).alias("next_quantity"),
        F.row_number().over(w).alias("seq_no"),
        F.lead("quantity", 2).over(w2).alias("next_quantity_2"),
    )
)

metrics = (
    enriched
    .filter(F.col("quantity").isNotNull())
    .select(
        "id",
        "store_id",
        "quantity",
        "next_quantity",
        "seq_no",
        F.max("seq_no").over(w3).alias("running_max_seq_no"),
        F.sum("seq_no").over(w3).alias("running_sum_seq_no"),
        F.min("seq_no").over(w3).alias("running_min_seq_no"),
        F.sum("seq_no").over(w4).alias("running_seq_no"),
    )
)

ranked = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

latest = (
    ranked
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

enriched_inventory = (
    latest
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.next_quantity",
        "src.running_sum_seq_no",
        "src.row_num",
        F.col("p.opened_date"),
        F.col("p.sqft"),
    )
)

scored = (
    enriched_inventory
    .select(
        "id",
        "store_id",
        "sqft",
        F.last("next_quantity", ignorenulls=True).over(w_part).alias("last_known_next_quantity"),
        F.count("next_quantity").over(w_part_2).alias("store_id_count_next_quantity"),
        F.sum("sqft").over(w_grp).alias("running_sqft"),
        F.percent_rank().over(w10).alias("running_sum_seq_no_percent_rank"),
    )
)

result = (
    scored
    .select("id", "store_id_count_next_quantity", "running_sum_seq_no_percent_rank")
    .orderBy("store_id_count_next_quantity")
)
