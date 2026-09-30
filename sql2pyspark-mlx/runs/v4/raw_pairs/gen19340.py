from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

cleaned = (
    df_products
    .filter(F.col("launch_date") > F.to_date(F.lit("2024-01-29")))
    .select(
        "id",
        "product_name",
        "category",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.date_format("launch_date", "yyyy-MM").alias("launch_date_label"),
        F.lower("brand").alias("brand_lower"),
        F.coalesce("brand", F.lit("DIRECT")).alias("brand_filled"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.launch_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock"), F.col("c.quantity"))
)

eligible = (
    with_last
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.order_id") == 39), "left_anti")
)

eligible_products = (
    eligible
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.order_id").isin([12, 13, 18, 27])), "left_semi")
)

result = (
    eligible_products
    .select(
        "id",
        "product_name",
        "category",
        "cost",
        "tags",
        "supplier_id",
        "brand_lower",
        "brand_filled",
        "quantity",
    )
)
