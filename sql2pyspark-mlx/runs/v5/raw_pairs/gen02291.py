from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("units", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("prev_units").asc_nulls_first())
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.min("quantity").alias("min_inventory_quantity"))

with_windows = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "units",
        F.lag("revenue").over(w).alias("prev_revenue"),
        F.lead("revenue").over(w).alias("next_revenue"),
        F.lead("units").over(w).alias("next_units"),
        F.lag("units").over(w).alias("prev_units"),
        F.row_number().over(w).alias("row_seq"),
    )
)

matched_keys = (
    with_windows
    .select("id", "store_id")
    .intersectAll(
        df_store_sales
        .filter(F.col("revenue").between(2300, 3300))
        .select("id", "store_id")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

enriched = reconciled.select("*", F.rank().over(w2).alias("prev_units_rank"))

enriched_store_sales = (
    enriched
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .select(
        "s.store_id",
        "s.units",
        "s.next_revenue",
        F.col("p.opened_date"),
        F.col("p.region"),
    )
)

with_dims = (
    enriched_store_sales
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "left")
    .filter(F.month(F.col("d.opened_date")).isin([3, 5, 11]))
    .select("a.store_id", "a.units", "a.region", F.col("d.sqft"))
)

result = (
    with_dims
    .join(quantity_by_store_id, on="store_id", how="left")
    .select("store_id", "units", "sqft", F.col("min_inventory_quantity"))
)
