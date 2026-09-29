from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")

scored = (
    df_inventory
    .select(
        "id",
        "store_id",
        "quantity",
        F.row_number().over(w).alias("seq_no"),
        F.ntile(4).over(w2).alias("quantity_bucket"),
    )
)

result = scored.select("id", "store_id", "quantity", "seq_no")
