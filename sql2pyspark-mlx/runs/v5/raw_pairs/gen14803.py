from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id")
w2 = Window.partitionBy("order_id").orderBy("rank_in_group", "id").rowsBetween(-6, -1)
w3 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("next_unit_price").desc(), "id")
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.avg("units").alias("avg_store_sale_units"))

base = (
    df_order_items
    .filter(F.col("order_id").isin([11, 18, 23]))
    .select(
        "*",
        F.greatest("quantity", "unit_price").alias("greatest_quantity_unit_price"),
    )
)

deduped = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    deduped
    .select(
        "id",
        "order_id",
        "product_id",
        "rank_in_group",
        F.lead("unit_price").over(w).alias("next_unit_price"),
        F.lag("quantity").over(w).alias("prev_quantity"),
        F.lead("quantity").over(w).alias("next_quantity"),
        F.row_number().over(w).alias("row_seq"),
        F.avg("quantity").over(w2).alias("quantity_avg_6_prior"),
    )
)

with_dims = (
    scored
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .filter(F.col("d.product_name").like("%Lapto%"))
    .select("src.*", F.col("d.price"))
)

enriched = (
    with_dims
    .select(
        "id",
        "product_id",
        "rank_in_group",
        "next_unit_price",
        "prev_quantity",
        "row_seq",
        "price",
        F.last("row_seq", ignorenulls=True).over(w3).alias("last_known_row_seq"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

result = (
    enriched
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "next_unit_price",
        "prev_quantity",
        "row_seq",
        "price",
        "last_known_row_seq",
        "event_seq",
        F.col("avg_store_sale_units"),
    )
)
