from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc())
w2 = Window.partitionBy("order_id").orderBy("quantity", "id")

result = (
    df_order_items
    .filter(F.col("order_id").isin([32, 34]))
    .select(
        "id",
        "order_id",
        "quantity",
        F.rank().over(w).alias("quantity_rank"),
        F.row_number().over(w2).alias("row_num"),
    )
)
