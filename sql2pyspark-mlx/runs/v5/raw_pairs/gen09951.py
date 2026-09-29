from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "order_date",
        "discount",
        F.lead("discount", 1).over(w).alias("next_discount"),
    )
    .orderBy(F.col("store_id").desc())
)
