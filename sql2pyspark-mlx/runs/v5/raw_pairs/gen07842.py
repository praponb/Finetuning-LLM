from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("last_restock").asc_nulls_last(), "id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id")
w3 = Window.partitionBy("store_id").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy(F.col("quantity_bucket").desc_nulls_last(), "id")
w5 = Window.orderBy(F.col("cumulative_quantity").desc())
w6 = Window.partitionBy("product_id")
w7 = Window.orderBy(F.col("quantity").desc(), "id")
w_grp = Window.partitionBy("store_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("product_id", "store_id").orderBy(F.col("cost").asc_nulls_first())
w10 = Window.partitionBy("product_id").orderBy("cost", "id")
w11 = Window.partitionBy("product_id").orderBy(F.col("product_id_avg_cumulative_quantity").desc(), "id")

deduped = (
    df_inventory
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

scored = (
    deduped
    .withColumn("quantity_bucket", F.ntile(10).over(w2))
    .withColumn("cumulative_quantity", F.sum("quantity").over(w3))
)

latest = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

enriched = (
    latest
    .filter(F.col("row_num") >= 1)
    .select(
        "id",
        "store_id",
        "product_id",
        "cumulative_quantity",
        F.percent_rank().over(w5).alias("cumulative_quantity_percent_rank"),
        F.avg("cumulative_quantity").over(w6).alias("product_id_avg_cumulative_quantity"),
        F.ntile(3).over(w7).alias("quantity_bucket_2"),
    )
)

joined = (
    enriched
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.product_id",
        "e.product_id_avg_cumulative_quantity",
        F.col("p.cost"),
    )
)

enriched_inventory = (
    joined
    .withColumn("cumulative_product_id_avg_cumulative_quantity", F.sum("product_id_avg_cumulative_quantity").over(w_grp))
    .withColumn("cost_cume_dist", F.cume_dist().over(w9))
    .withColumn("seq_no", F.row_number().over(w10))
    .withColumn("row_seq", F.row_number().over(w11))
)

result = (
    enriched_inventory
    .groupBy(
        F.when(F.col("cumulative_product_id_avg_cumulative_quantity") >= 5852, "HIGH").otherwise("LOW").alias("cumulative_product_id_avg_cumulative_quantity_tier"),
        F.when(F.col("seq_no") >= 7, "HIGH").otherwise("LOW").alias("seq_no_tier"),
    )
    .agg(
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy("seq_no_tier")
)
