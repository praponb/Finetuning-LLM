from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("category").orderBy("greatest_quantity_unit_price", "id")
w3 = Window.partitionBy("product_brand").orderBy("greatest_quantity_unit_price", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("product_brand").orderBy("greatest_quantity_unit_price")

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("quantity").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.greatest("quantity", "unit_price").alias("greatest_quantity_unit_price"),
    )
)

with_dims = (
    valid
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .filter(F.array_contains(F.col("p.tags"), "premium"))
    .select("e.*", F.col("p.product_name"), F.col("p.category"), F.col("p.brand"))
)

enriched = (
    with_dims
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.product_id",
        "s.quantity",
        "s.greatest_quantity_unit_price",
        "s.product_name",
        "s.category",
        "s.brand",
        F.col("d.brand").alias("product_brand"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.sum("greatest_quantity_unit_price").over(w).alias("running_greatest_quantity_unit_price"),
        F.row_number().over(w2).alias("seq_no"),
        F.avg("greatest_quantity_unit_price").over(w3).alias("greatest_quantity_unit_price_avg_2"),
        F.percent_rank().over(w4).alias("greatest_quantity_unit_price_percent_rank"),
    )
)

result = (
    scored
    .select(
        "id",
        "quantity",
        "greatest_quantity_unit_price",
        "product_brand",
        "running_greatest_quantity_unit_price",
        "seq_no",
        "greatest_quantity_unit_price_avg_2",
        "greatest_quantity_unit_price_percent_rank",
        F.when(F.col("product_brand").rlike("^(Stark|Acme)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("product_brand_zone"),
    )
    .orderBy(F.col("greatest_quantity_unit_price").asc_nulls_first(), "seq_no")
)
