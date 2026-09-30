from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("cost", "id")
w_recent = Window.partitionBy("supplier_id").orderBy("price", "id")

result = (
    df_products
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.ntile(5).over(w_recent).alias("price_bucket"),
    )
)
