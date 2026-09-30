from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id")
w2 = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_restock").desc(), "id")
w3 = Window.partitionBy("store_id").orderBy("launch_date", "id")
w4 = Window.partitionBy("store_id").orderBy(F.col("seq_no").desc(), "id")

enriched = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.lag("quantity").over(w).alias("prev_quantity"),
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

enriched_inventory = (
    enriched
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.launch_date"), F.col("p.cost"))
)

enriched_inventory_2 = (
    enriched_inventory
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.last_restock",
        "e.seq_no",
        "e.launch_date",
        "e.cost",
        F.col("s.sqft"),
        F.col("s.city"),
        F.col("s.store_name"),
    )
)

with_windows = (
    enriched_inventory_2
    .select(
        "id",
        "launch_date",
        "sqft",
        "store_name",
        F.lag("seq_no").over(w3).alias("prev_seq_no"),
        F.lead("seq_no").over(w3).alias("next_seq_no"),
        F.lead("sqft").over(w3).alias("next_sqft"),
        F.lag("sqft").over(w3).alias("prev_sqft"),
        F.row_number().over(w3).alias("seq_no_2"),
        F.ntile(5).over(w4).alias("seq_no_bucket"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("next_sqft") >= 15576, "HIGH").otherwise("LOW").alias("next_sqft_tier"),
        "prev_seq_no",
    )
    .agg(
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.round(F.avg("next_seq_no"), 2).alias("avg_next_seq_no"),
    )
    .filter(F.col("avg_sqft") >= 0)
)
