from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("price")
w2 = Window.partitionBy("supplier_id")
w3 = Window.partitionBy("brand")

valid = (
    df_products
    .filter(F.col("launch_date") < "2023-10-01")
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        (F.col("cost") + F.col("price")).alias("margin"),
    )
)

kept_keys = (
    valid
    .select("category")
    .intersectAll(
        df_products
        .filter(F.col("supplier_id").isNull())
        .select("category")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .select(
        "*",
        F.percent_rank().over(w).alias("price_percent_rank"),
        F.round((F.col("price") / F.sum("price").over(w2)), 4).alias("price_share"),
        F.corr("price", "cost").over(w3).alias("corr_price_cost"),
    )
)

result = (
    with_windows
    .select(
        "brand",
        F.col("price").alias("PRICE_VAL"),
        F.col("price_share").alias("PRICE_SHARE_VAL"),
        F.col("margin").alias("MARGIN"),
    )
    .unpivot(["brand"], ["PRICE_VAL", "PRICE_SHARE_VAL", "MARGIN"], "kpi", "val")
    .filter(F.col("val").isNotNull())
)
