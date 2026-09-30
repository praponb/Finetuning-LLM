from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("order_date").desc(), "id")

result = (
    df_orders
    .filter(F.col("amount").isNotNull())
    .select(
        "id",
        "store_id",
        "order_date",
        "discount",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 3)
)
