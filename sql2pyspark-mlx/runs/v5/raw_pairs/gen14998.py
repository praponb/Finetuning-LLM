from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("supplier_id")
w_recent = Window.orderBy("unit_price", "id")
w_running = Window.partitionBy("supplier_id").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("unit_price").asc_nulls_last())

cleaned = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2023-10-02").cast("date"))
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.regexp_replace("product_name", " ", "-").alias("product_name_slug"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("resolved_brand"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_id",
        "s.resolved_brand",
        F.col("c.quantity"),
        F.col("c.unit_price"),
    )
)

joined = (
    latest_match
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id").eqNullSafe(F.col("p.id")), "inner")
    .select(
        "e.id",
        "e.supplier_id",
        "e.resolved_brand",
        "e.unit_price",
        F.col("p.supplier_name"),
        F.col("p.city"),
        F.col("p.rating"),
    )
)

with_dims = (
    joined
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id") == F.col("p.id"), "left")
    .filter(F.col("p.country") == "US")
    .select("e.*", F.col("p.rating").alias("supplier_rating"))
)

result = (
    with_dims
    .select(
        "id",
        "supplier_id",
        "resolved_brand",
        "supplier_name",
        "rating",
        "supplier_rating",
        F.round((F.col("unit_price") / F.sum("unit_price").over(w2)), 4).alias("unit_price_share"),
        F.lead("supplier_rating", 1).over(w_recent).alias("next_supplier_rating"),
        F.sum("supplier_rating").over(w_running).alias("cumulative_supplier_rating"),
        F.cume_dist().over(w5).alias("unit_price_cume_dist"),
        F.round((F.col("rating") / F.sum("rating").over(w2)), 4).alias("rating_share"),
    )
)
