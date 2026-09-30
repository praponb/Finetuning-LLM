from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-3, -1)

metrics = (
    df_order_items
    .select(
        "id",
        "unit_price",
        F.lag("quantity").over(w).alias("prev_quantity"),
        F.lead("quantity").over(w).alias("next_quantity"),
        F.lead("unit_price").over(w).alias("next_unit_price"),
        F.lag("unit_price").over(w).alias("prev_unit_price"),
        F.row_number().over(w).alias("event_seq"),
        F.avg("unit_price").over(w2).alias("unit_price_avg_3_prior"),
    )
)

result = (
    metrics
    .select(
        "id",
        "unit_price",
        "prev_quantity",
        "next_quantity",
        "next_unit_price",
        "prev_unit_price",
        "event_seq",
        "unit_price_avg_3_prior",
    )
)
