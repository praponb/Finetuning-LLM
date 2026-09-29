from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("discount", "id")

result = (
    df_orders
    .filter(F.col("shipping_fee") >= 24)
    .select(
        "id",
        "store_id",
        "discount",
        "order_date",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
    .orderBy("id", "pos")
)
