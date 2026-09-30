from pyspark.sql import functions as F

result = (
    df_order_items
    .select("id")
    .exceptAll(
        df_order_items
        .filter(F.col("unit_price") <= 240)
        .select("id")
    )
)
