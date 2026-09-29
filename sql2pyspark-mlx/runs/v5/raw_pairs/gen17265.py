from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")

metrics = (
    df_inventory
    .select(
        "id",
        "product_id",
        F.min("quantity").over(w).alias("running_min_quantity"),
        F.avg("quantity").over(w).alias("running_avg_quantity"),
        F.max("quantity").over(w).alias("running_max_quantity"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

result = (
    metrics
    .select(
        "id",
        "product_id",
        "running_min_quantity",
        "running_max_quantity",
        "seq_no",
    )
    .orderBy(F.col("running_min_quantity").asc_nulls_last(), "running_max_quantity")
)
