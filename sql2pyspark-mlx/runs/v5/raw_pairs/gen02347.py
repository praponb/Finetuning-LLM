from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id").rowsBetween(-3, Window.currentRow)

enriched = (
    df_inventory
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.min("quantity").over(w_recent).alias("last4_min_quantity"),
        F.max("quantity").over(w_recent).alias("last4_max_quantity"),
        F.sum("quantity").over(w_recent).alias("last4_sum_quantity"),
        F.count("quantity").over(w_recent).alias("last4_count_quantity"),
    )
)

result = (
    enriched
    .rollup(F.date_trunc("WEEK", "last_restock").alias("week_start"))
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.round(F.avg("last4_max_quantity"), 2).alias("avg_last4_max_quantity"),
    )
)
