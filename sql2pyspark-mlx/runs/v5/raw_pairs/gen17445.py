from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("supplier_id").orderBy("cost", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("category", "supplier_id")

kept = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.order_id").isin([5, 15, 32, 33])), "left_anti")
)

metrics = (
    kept
    .filter(F.col("stock_qty") < 362)
    .select(
        "id",
        "category",
        "launch_date",
        "tags",
        "supplier_id",
        F.avg("stock_qty").over(w_grp).alias("last3_avg_stock_qty"),
        F.min("stock_qty").over(w_grp).alias("last3_min_stock_qty"),
        F.max("stock_qty").over(w_grp).alias("last3_max_stock_qty"),
    )
)

enriched = (
    metrics
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.supplier_name"), F.col("p.rating"), F.col("p.country"))
)

matched_keys = (
    enriched
    .select("supplier_id", "id")
    .intersect(
        df_products
        .filter(F.col("supplier_id") == 25)
        .select("supplier_id", "id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "category",
        "launch_date",
        "tags",
        "supplier_id",
        "last3_avg_stock_qty",
        "last3_max_stock_qty",
        "supplier_name",
        "country",
        F.count("last3_max_stock_qty").over(w2).alias("category_count_last3_max_stock_qty"),
    )
)
