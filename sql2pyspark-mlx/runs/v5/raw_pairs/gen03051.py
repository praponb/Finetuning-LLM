from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("category", "brand").orderBy("stock_qty", "id")
w_running_2 = Window.partitionBy("country")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("supplier_id", "country").orderBy(F.col("inventory_last_restock").desc(), "id")
w5 = Window.partitionBy("country", "category").orderBy(F.col("prev_stock_qty").desc_nulls_first())
w6 = Window.orderBy("inventory_last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("supplier_id").orderBy("row_seq_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("supplier_id")
w9 = Window.partitionBy("supplier_id").orderBy("next_stock_qty", "id").rowsBetween(-5, -1)

enriched = (
    df_products
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id").eqNullSafe(F.col("d.id")), "left")
    .filter(~F.col("d.country").isin(["SG", "TH", "US"]))
    .select(
        "e.id",
        "e.product_name",
        "e.category",
        "e.brand",
        "e.cost",
        "e.stock_qty",
        "e.supplier_id",
        F.col("d.country"),
        F.col("d.city"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "category",
        "cost",
        "stock_qty",
        "supplier_id",
        "country",
        F.lag("stock_qty").over(w_running).alias("prev_stock_qty"),
        F.lead("stock_qty").over(w_running).alias("next_stock_qty"),
        F.lag("cost").over(w_running).alias("prev_cost"),
        F.lead("cost").over(w_running).alias("next_cost"),
        F.row_number().over(w_running).alias("row_seq"),
        F.avg("cost").over(w_running_2).alias("country_avg_cost"),
    )
)

valid = (
    with_windows
    .filter(F.col("cost") >= 220)
    .select(
        "id",
        "category",
        "stock_qty",
        "supplier_id",
        "country",
        "prev_stock_qty",
        "next_stock_qty",
        "prev_cost",
        "next_cost",
        "row_seq",
        "country_avg_cost",
        F.ceil("row_seq").alias("row_seq_whole"),
    )
)

with_latest = (
    valid
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock").alias("inventory_last_restock"))
)

ranked = (
    with_latest
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    ranked
    .withColumn("prev_stock_qty_dense_rank", F.dense_rank().over(w5))
    .withColumn("cumulative_country_avg_cost", F.sum("country_avg_cost").over(w6))
    .withColumn("last_known_row_seq_whole", F.last("row_seq_whole", ignorenulls=True).over(w7))
    .withColumn("next_cost_share", F.round((F.col("next_cost") / F.sum("next_cost").over(w8)), 4))
    .withColumn("stock_qty_avg_5_prior", F.avg("stock_qty").over(w9))
)

daily = (
    scored
    .groupBy(
        F.when(F.col("stock_qty") >= 424, "HIGH").otherwise("LOW").alias("stock_qty_tier"),
    )
    .agg(
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
        F.percentile_approx("cumulative_country_avg_cost", [0.25, 0.5, 0.75]).alias("cumulative_country_avg_cost_quartiles"),
    )
    .filter(F.col("stock_qty_bit_and") > 1)
)

result = (
    daily
    .select(
        "stock_qty_tier",
        "stock_qty_bit_and",
        "cumulative_country_avg_cost_quartiles",
    )
    .orderBy("stock_qty_tier")
    .limit(20)
)
