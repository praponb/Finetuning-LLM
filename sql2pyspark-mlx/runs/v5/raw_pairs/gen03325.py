from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("discount").desc(), "id")

result = (
    df_orders
    .filter(F.col("status").isin(["PENDING", "RETURNED"]))
    .select(
        "id",
        "status",
        "discount",
        "order_date",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
    .select("id", "status", "discount", "order_date")
    .orderBy(F.col("status").desc())
)
