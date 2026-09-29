from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.orderBy("quantity_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-06-10").cast("date"))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.date_add("last_restock", 15).alias("last_restock_plus_15d"),
        F.col("quantity").cast("double").alias("quantity_num"),
    )
)

enriched = (
    base
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.cost"))
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_windows = (
    latest
    .withColumn("first_known_quantity", F.first("quantity", ignorenulls=True).over(w2))
    .withColumn("quantity_avg_2", F.avg("quantity").over(w_ordered))
    .withColumn("running_cost", F.sum("cost").over(w4))
    .withColumn("running_quantity", F.sum("quantity").over(w_running))
)

result = (
    with_windows
    .groupBy("product_id")
    .agg(
        F.percentile_approx("running_cost", 0.95).alias("running_cost_p95"),
        F.max("quantity_num").alias("max_quantity_num"),
    )
    .orderBy(F.col("running_cost_p95").desc())
)
