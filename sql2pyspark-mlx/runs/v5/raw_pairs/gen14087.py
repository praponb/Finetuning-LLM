from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)
