from pyspark.sql import functions as F

result = (
    df_order_items
    .select("order_id", "id")
    .intersectAll(
        df_order_items
        .filter(F.col("unit_price").isNotNull())
        .select("order_id", "id")
    )
)
