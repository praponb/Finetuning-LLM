from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") == 7)
    .select("id", "order_id")
    .subtract(
        df_order_items
        .filter(F.col("order_id").isin([2, 4, 19]))
        .select("id", "order_id")
    )
    .orderBy("order_id")
)
