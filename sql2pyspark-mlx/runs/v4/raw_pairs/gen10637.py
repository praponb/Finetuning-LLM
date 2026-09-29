from pyspark.sql import functions as F

result = (
    df_shipments
    .select("cost")
    .subtract(
        df_order_items
        .filter(F.col("quantity").between(4, 8))
        .select("unit_price")
    )
)
