from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc())

scored = (
    df_inventory
    .select(
        "*",
        F.min("quantity").over(w_hist).alias("last7_min_quantity"),
        F.max("quantity").over(w_hist).alias("last7_max_quantity"),
        F.count("quantity").over(w_hist).alias("last7_count_quantity"),
        F.dense_rank().over(w2).alias("quantity_dense_rank"),
    )
)

result = (
    scored
    .select(
        "id",
        "last7_min_quantity",
        "last7_max_quantity",
        "last7_count_quantity",
        "quantity_dense_rank",
    )
)
