from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

base = (
    df_products
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-05-29"))) & (F.col("brand") == "Initech"))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.sort_array("tags").alias("sorted_tags"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("resolved_brand"),
    )
)

eligible = (
    base
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") < F.col("s.launch_date")), "left_anti")
)

filtered = (
    eligible
    .filter(F.col("cost").between(450, 490))
    .select(
        "*",
        F.date_trunc("year", "launch_date").alias("launch_date_period"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("resolved_brand_2"),
    )
)

with_last = (
    filtered
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.unit_price").alias("order_item_unit_price"),
        F.col("c.quantity"),
    )
)

result = (
    with_last
    .select(
        "id",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "launch_date_period",
        "order_item_unit_price",
    )
    .orderBy(F.col("launch_date_period").asc_nulls_last())
)
