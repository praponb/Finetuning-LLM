from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("cost").desc_nulls_first())
w_part = Window.orderBy("stock_qty_whole", "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("quantity_adjusted").desc(), "id")
w_grp = Window.partitionBy("order_id").orderBy("stock_qty_whole_plus_quantity", "id").rowsBetween(-2, Window.currentRow)

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("quantity").between(4, 7))
    .select(
        "*",
        F.when(F.col("unit_price") >= 350, "High").when(F.col("unit_price") >= 170, "Medium").otherwise("Low").alias("unit_price_tier"),
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
    )
)

enriched = (
    cleaned
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.product_id",
        "s.quantity",
        "s.line_total",
        F.col("p.stock_qty"),
        F.col("p.cost"),
    )
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "order_id", "quantity", "stock_qty", "cost")
)

valid = (
    ranked
    .filter(F.col("order_id") == 36)
    .select(
        "id",
        "order_id",
        "quantity",
        F.when(F.col("stock_qty") >= 446, "High").when(F.col("stock_qty") >= 190, "Medium").otherwise("Low").alias("stock_qty_tier"),
        F.floor("stock_qty").alias("stock_qty_whole"),
    )
)

filtered = (
    valid
    .filter(F.col("stock_qty_whole") != 26)
    .withColumn("quantity_adjusted", F.round((F.col("quantity") * 1.07), 2))
    .withColumn("stock_qty_whole_plus_quantity", (F.col("stock_qty_whole") + F.col("quantity")))
)

scored = (
    filtered
    .filter(F.col("stock_qty_whole_plus_quantity").between(58, 75))
    .select(
        "id",
        "quantity",
        F.lead("stock_qty_whole", 1).over(w_part).alias("next_stock_qty_whole"),
        F.row_number().over(w3).alias("seq_no"),
        F.avg("quantity_adjusted").over(w_grp).alias("quantity_adjusted_avg_2"),
        F.lag("quantity_adjusted", 1).over(w_part).alias("prev_quantity_adjusted"),
    )
)

result = (
    scored
    .cube("seq_no")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.min("next_stock_qty_whole").alias("min_next_stock_qty_whole"),
        F.percentile_approx("prev_quantity_adjusted", 0.95).alias("prev_quantity_adjusted_p95"),
        F.count("*").alias("row_count"),
        F.percentile_approx("quantity_adjusted_avg_2", [0.25, 0.5, 0.75]).alias("quantity_adjusted_avg_2_quartiles"),
    )
    .orderBy(F.col("prev_quantity_adjusted_p95").asc_nulls_last())
)
