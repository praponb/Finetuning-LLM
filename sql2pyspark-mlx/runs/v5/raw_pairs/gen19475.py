from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("shipped_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "shipped_date",
        "cost",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .orderBy("id", "row_num")
)
