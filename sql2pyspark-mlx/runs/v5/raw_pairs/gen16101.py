from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("unit_price", "id")
w3 = Window.partitionBy("product_id", "order_id")

scored = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.avg("unit_price").over(w).alias("unit_price_avg_6"),
        F.row_number().over(w2).alias("event_seq"),
        F.corr("quantity", "unit_price").over(w3).alias("corr_quantity_unit_price"),
    )
)

result = (
    scored
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price_avg_6",
        "event_seq",
        "corr_quantity_unit_price",
    )
    .orderBy(F.col("unit_price_avg_6").desc())
)
