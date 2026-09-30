from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "supplier_id").orderBy("cost", "id")
w2 = Window.partitionBy("brand").orderBy("stock_qty", "id")

metrics = (
    df_products
    .select(
        "*",
        F.ntile(3).over(w).alias("cost_bucket"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

result = (
    metrics
    .select(
        "id",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        "cost_bucket",
        "seq_no",
    )
    .orderBy(F.col("supplier_id").desc())
)
