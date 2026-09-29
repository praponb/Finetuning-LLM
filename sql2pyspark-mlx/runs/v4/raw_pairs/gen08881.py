from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("unit_price").desc_nulls_last())
w2 = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")

result = (
    df_order_items
    .select(
        "*",
        F.percent_rank().over(w).alias("unit_price_percent_rank"),
        F.row_number().over(w2).alias("seq_no"),
    )
)
