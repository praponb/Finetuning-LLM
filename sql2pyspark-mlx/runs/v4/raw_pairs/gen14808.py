from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy(F.col("price").desc_nulls_last())
w2 = Window.partitionBy("supplier_id").orderBy("dedup_rank", "id")
w_running = Window.orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("category", "supplier_id").orderBy("price", "id")
w5 = Window.partitionBy("brand").orderBy(F.col("cost").desc(), "id")
w6 = Window.partitionBy("supplier_id").orderBy("price", "id")
w7 = Window.partitionBy("supplier_id")
w8 = Window.orderBy("supplier_rating_2")
w9 = Window.orderBy(F.col("stock_qty").asc_nulls_first())

latest = (
    df_products
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    latest
    .select(
        "*",
        F.lag("price", 2).over(w2).alias("prev_price"),
        F.last("stock_qty", ignorenulls=True).over(w_running).alias("last_known_stock_qty"),
        F.row_number().over(w_part).alias("seq_no"),
        F.ntile(3).over(w5).alias("cost_bucket"),
        F.lead("stock_qty", 2).over(w6).alias("next_stock_qty"),
    )
)

kept_keys = (
    enriched
    .select("id")
    .subtract(
        df_products
        .filter(F.size("tags") >= 1)
        .select("id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched_products = (
    reconciled
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.country"), F.col("d.city"), F.col("d.rating"))
)

enriched_products_2 = (
    enriched_products
    .alias("a")
    .join(df_suppliers.alias("b"), F.col("a.supplier_id") == F.col("b.id"), "left")
    .select(
        "a.*",
        F.col("b.rating").alias("supplier_rating"),
        F.col("b.city").alias("supplier_city"),
    )
)

enriched_products_3 = (
    enriched_products_2
    .alias("src")
    .join(df_suppliers.alias("s"), F.col("src.supplier_id") == F.col("s.id"), "left")
    .select(
        "src.*",
        F.col("s.country").alias("supplier_country"),
        F.col("s.city").alias("supplier_city_2"),
        F.col("s.rating").alias("supplier_rating_2"),
    )
)

with_windows = (
    enriched_products_3
    .select(
        "*",
        F.corr("cost", "price").over(w7).alias("corr_cost_price"),
        F.cume_dist().over(w8).alias("supplier_rating_2_cume_dist"),
        F.percent_rank().over(w9).alias("stock_qty_percent_rank"),
    )
)

result = (
    with_windows
    .select(
        "supplier_city_2",
        "stock_qty_percent_rank",
        "supplier_rating_2_cume_dist",
        "supplier_rating",
        "supplier_rating_2",
    )
    .unpivot(
        ["supplier_city_2"],
        ["stock_qty_percent_rank", "supplier_rating_2_cume_dist", "supplier_rating", "supplier_rating_2"],
        "metric_name",
        "val",
    )
)
