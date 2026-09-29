from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_last(), "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy("row_num", "id").rowsBetween(-6, Window.currentRow)
w_running = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id")
w4 = Window.partitionBy("store_id")
w5 = Window.orderBy("quantity_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_inventory
    .filter(F.col("product_id").isin([4, 5, 7, 9]))
    .select("*", F.ceil("quantity").alias("quantity_whole"))
)

deduped = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

scored = (
    deduped
    .filter(F.col("quantity_whole").isNotNull())
    .select(
        "id",
        "product_id",
        "last_restock",
        F.avg("quantity").over(w2).alias("quantity_avg_6"),
        F.row_number().over(w_running).alias("row_seq"),
        F.count("quantity").over(w4).alias("store_id_count_quantity"),
        F.last("quantity_whole", ignorenulls=True).over(w5).alias("last_known_quantity_whole"),
    )
)

enriched = (
    scored
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.stock_qty"))
)

base_inventory = (
    enriched
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-02-17")))
    .withColumn("quantity_avg_6_adjusted", F.round((F.col("quantity_avg_6") * 1.1), 2))
)

result = (
    base_inventory
    .groupBy(F.date_trunc("month", "last_restock").alias("activity_month"))
    .agg(
        F.round(F.avg("quantity_avg_6"), 2).alias("avg_quantity_avg_6"),
        F.percentile_approx("row_seq", 0.95).alias("row_seq_p95"),
        F.percentile_approx("store_id_count_quantity", [0.25, 0.5, 0.75]).alias("store_id_count_quantity_quartiles"),
    )
    .orderBy(F.col("activity_month").asc_nulls_last())
)
