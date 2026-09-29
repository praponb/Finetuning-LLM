from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(-3, Window.currentRow)
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.sum("quantity").alias("sum_inventory_quantity"))

with_dims = (
    df_inventory
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .filter(F.col("b.product_name").like("S%"))
    .select("a.*", F.col("b.brand"))
)

scored = (
    with_dims
    .withColumn("next_quantity", F.lead("quantity").over(w))
    .withColumn("prev_quantity", F.lag("quantity").over(w))
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("quantity_avg_3", F.avg("quantity").over(w2))
)

filtered = (
    scored
    .filter(F.col("brand").isNotNull() & F.col("quantity").between(63, 165))
    .withColumn("quantity_avg_3_filled", F.coalesce("quantity_avg_3", F.lit(0)))
    .withColumn("quantity_avg_3_num", F.col("quantity_avg_3").cast("double"))
)

remaining_keys = (
    filtered
    .select("product_id")
    .intersectAll(
        df_inventory
        .filter(F.col("product_id").isin([2, 6]))
        .select("product_id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

with_dims_inventory = (
    reconciled
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.quantity",
        "a.last_restock",
        "a.brand",
        "a.next_quantity",
        "a.prev_quantity",
        "a.row_seq",
        "a.quantity_avg_3",
        "a.quantity_avg_3_num",
        F.col("p.city"),
    )
)

enriched = (
    with_dims_inventory
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.last_restock",
        "src.prev_quantity",
        "src.row_seq",
        "src.quantity_avg_3",
        "src.quantity_avg_3_num",
        "src.city",
        F.col("d.region"),
        F.col("d.sqft"),
    )
)

valid = (
    enriched
    .filter(F.col("sqft") < 11467)
    .select(
        "*",
        (F.col("sqft") * F.col("row_seq")).alias("sqft_times_row_seq"),
        F.coalesce("city", F.lit("OTHER")).alias("city_filled"),
    )
)

result = (
    valid
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "last_restock",
        "row_seq",
        "quantity_avg_3",
        "quantity_avg_3_num",
        "city",
        "region",
        "sqft",
        F.col("sum_inventory_quantity"),
    )
)
