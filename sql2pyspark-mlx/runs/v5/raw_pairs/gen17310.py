from pyspark.sql import functions as F

result = (
    df_order_items
    .select("order_id", "id")
    .subtract(
        df_order_items
        .filter(F.col("quantity") != 5)
        .select("order_id", "id")
    )
)
