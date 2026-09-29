from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock", "id")
w2 = Window.partitionBy("store_id").orderBy("quantity", "id")
w3 = Window.orderBy("quantity", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w_part = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("quantity_avg_2_prior", "id")
w7 = Window.orderBy("cumulative_quantity", "id")
w_running = Window.partitionBy("store_id").orderBy(F.col("quantity_avg_2_prior").desc_nulls_last())
w9 = Window.partitionBy("product_id").orderBy(F.col("prev_quantity").desc(), "id")
w10 = Window.partitionBy("product_id").orderBy(F.col("cost").desc(), "id")
w11 = Window.orderBy("prev_next_quantity", "id").rowsBetween(-3, Window.currentRow)

metrics = (
    df_inventory
    .withColumn("prev_quantity", F.lag("quantity", 2).over(w))
    .withColumn("next_quantity", F.lead("quantity", 2).over(w2))
    .withColumn("quantity_avg_2_prior", F.avg("quantity").over(w3))
    .withColumn("quantity_avg_2", F.avg("quantity").over(w4))
    .withColumn("cumulative_quantity", F.sum("quantity").over(w_part))
)

with_dims = (
    metrics
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.cost"))
)

scored = (
    with_dims
    .select(
        "*",
        F.lag("cost", 1, 0).over(w6).alias("prev_cost"),
        F.lag("next_quantity", 1).over(w7).alias("prev_next_quantity"),
        F.cume_dist().over(w_running).alias("quantity_avg_2_prior_cume_dist"),
        F.row_number().over(w9).alias("row_seq"),
    )
)

metrics_inventory = (
    scored
    .withColumn("row_seq_2", F.row_number().over(w10))
    .withColumn("prev_next_quantity_avg_3", F.avg("prev_next_quantity").over(w11))
)

result = (
    metrics_inventory
    .groupBy(
        F.date_trunc("QUARTER", "last_restock").alias("quarter_start"),
        F.date_trunc("WEEK", "last_restock").alias("week_start"),
    )
    .agg(
        F.round(F.avg("cumulative_quantity"), 2).alias("avg_cumulative_quantity"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.count("*").alias("row_count"),
        F.bit_or("quantity").alias("quantity_bit_or"),
    )
)
