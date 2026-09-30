from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("supplier_id").orderBy("price", "id")
w_grp = Window.partitionBy("category").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("category", "brand").orderBy("next_stock_qty", "id")
w_hist = Window.orderBy(F.col("prev_stock_qty").desc_nulls_first())
w_hist_2 = Window.orderBy(F.col("last_known_cost").asc_nulls_first())
w6 = Window.orderBy(F.col("price").asc_nulls_last())
w7 = Window.orderBy(F.col("next_stock_qty_2").desc())
w_part = Window.orderBy(F.col("next_stock_qty_2").desc_nulls_first())
w9 = Window.orderBy(F.col("last_known_cost").desc_nulls_last())
w_running = Window.orderBy("last_known_cost")
w_ordered = Window.orderBy(F.col("price_percent_rank").asc_nulls_first())

joined = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.city"), F.col("s.country"), F.col("s.supplier_name"))
)

scored = (
    joined
    .filter(F.col("country") == "DE")
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "supplier_name",
        F.lag("stock_qty").over(w_recent).alias("prev_stock_qty"),
        F.lead("stock_qty").over(w_recent).alias("next_stock_qty"),
        F.lag("price").over(w_recent).alias("prev_price"),
        F.lead("price").over(w_recent).alias("next_price"),
        F.row_number().over(w_recent).alias("seq_no"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "price",
        "prev_stock_qty",
        "seq_no",
        F.last("cost", ignorenulls=True).over(w_grp).alias("last_known_cost"),
        F.lead("stock_qty", 2, 0).over(w3).alias("next_stock_qty_2"),
    )
)

kept_keys = (
    enriched
    .select("id")
    .exceptAll(
        df_products
        .filter(F.col("cost") < 570)
        .select("id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .select(
        "last_known_cost",
        "next_stock_qty_2",
        F.percent_rank().over(w_hist).alias("prev_stock_qty_percent_rank"),
        F.rank().over(w_hist_2).alias("last_known_cost_rank"),
        F.percent_rank().over(w6).alias("price_percent_rank"),
    )
)

result = (
    metrics
    .select(
        "next_stock_qty_2",
        "prev_stock_qty_percent_rank",
        "last_known_cost_rank",
        F.dense_rank().over(w7).alias("next_stock_qty_2_dense_rank"),
        F.dense_rank().over(w_part).alias("next_stock_qty_2_dense_rank_2"),
        F.rank().over(w9).alias("last_known_cost_rank_2"),
        F.dense_rank().over(w_running).alias("last_known_cost_dense_rank"),
        F.cume_dist().over(w_ordered).alias("price_percent_rank_cume_dist"),
    )
)
