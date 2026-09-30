from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc(), "id")
w3 = Window.partitionBy("brand").orderBy("unit_price", "id")
w4 = Window.partitionBy("brand").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (~F.col("brand").eqNullSafe(F.lit("Acme"))))
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        F.array_contains("tags", "eco").alias("is_eco"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("resolved_brand"),
    )
)

with_latest = (
    base
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.brand",
        "s.price",
        "s.cost",
        "s.stock_qty",
        "s.launch_date",
        "s.tags",
        "s.is_eco",
        "s.resolved_brand",
        F.col("c.unit_price"),
    )
)

eligible = (
    with_latest
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id").isin([18, 25, 39])), "left_anti")
)

with_windows = (
    eligible
    .select(
        "*",
        F.row_number().over(w2).alias("seq_no"),
        F.lead("price", 1, 0).over(w3).alias("next_price"),
        F.first("cost", ignorenulls=True).over(w4).alias("first_known_cost"),
        F.first("unit_price", ignorenulls=True).over(w5).alias("first_known_unit_price"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "is_eco",
        "unit_price",
        "seq_no",
        "first_known_cost",
    )
)
