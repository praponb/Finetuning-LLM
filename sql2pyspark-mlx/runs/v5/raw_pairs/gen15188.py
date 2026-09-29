from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("order_id").orderBy("delivered_date", "id")
w2 = Window.orderBy(F.col("cost").desc_nulls_last())

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "carrier",
        "shipped_date",
        "status",
        F.lag("cost").over(w_hist).alias("prev_cost"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.percent_rank().over(w2).alias("cost_percent_rank"),
    )
)
