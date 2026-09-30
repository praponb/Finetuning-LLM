from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc())

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        F.dense_rank().over(w).alias("quantity_dense_rank"),
    )
)
