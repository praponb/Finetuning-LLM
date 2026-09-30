from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("quantity", "id").rowsBetween(-5, -1)
w2 = Window.orderBy(F.col("quantity").desc(), "id")
w3 = Window.partitionBy("category").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("category").orderBy("quantity", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("category").orderBy(F.col("stock_qty_avg_3").asc_nulls_last(), "id")
w6 = Window.partitionBy("category").orderBy("quantity")
w7 = Window.orderBy("quantity", "id")

base = (
    df_inventory
    .filter((F.col("last_restock") > F.lit("2023-04-30").cast("date")) & F.col("product_id").isin([4, 9, 10]))
    .withColumn("last_restock_month_end", F.last_day("last_restock"))
)

scored = (
    base
    .select(
        "*",
        F.avg("quantity").over(w_recent).alias("quantity_avg_5_prior"),
        F.ntile(5).over(w2).alias("quantity_bucket"),
    )
)

with_dims = (
    scored
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "inner")
    .select(
        "a.id",
        "a.product_id",
        "a.quantity",
        "a.quantity_avg_5_prior",
        F.col("b.stock_qty"),
        F.col("b.category"),
    )
)

with_windows = (
    with_dims
    .select(
        "id",
        "quantity",
        "quantity_avg_5_prior",
        "category",
        F.first("quantity_avg_5_prior", ignorenulls=True).over(w3).alias("first_known_quantity_avg_5_prior"),
        F.avg("stock_qty").over(w4).alias("stock_qty_avg_3"),
    )
)

filtered = (
    with_windows
    .filter(F.col("category").isNotNull())
    .select(
        "id",
        "quantity",
        "quantity_avg_5_prior",
        "category",
        "stock_qty_avg_3",
        F.round("quantity", 2).alias("quantity_rounded"),
    )
)

deduped = (
    filtered
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

scored_inventory = (
    deduped
    .filter(~F.col("category").isin(["Grocery", "Electronics"]))
    .select(
        "id",
        "quantity_avg_5_prior",
        "category",
        "stock_qty_avg_3",
        "rn",
        F.dense_rank().over(w6).alias("quantity_dense_rank"),
        F.lead("quantity_rounded", 1).over(w7).alias("next_quantity_rounded"),
    )
)

result = (
    scored_inventory
    .select(
        "id",
        "category",
        "stock_qty_avg_3",
        "quantity_dense_rank",
        "next_quantity_rounded",
    )
)
