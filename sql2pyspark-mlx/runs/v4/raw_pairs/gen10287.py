from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.orderBy(F.col("quantity").desc(), "id")

result = (
    df_order_items
    .select(
        "*",
        F.round((F.col("quantity") / F.sum("quantity").over(w)), 4).alias("quantity_share"),
        F.row_number().over(w2).alias("event_seq"),
    )
)
