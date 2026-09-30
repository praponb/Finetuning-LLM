from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("status").orderBy("delivered_date", "id")
w2 = Window.partitionBy("carrier").orderBy("cost", "id")

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.lead("cost").over(w_hist).alias("next_cost"),
        F.row_number().over(w_hist).alias("seq_no"),
        F.ntile(4).over(w2).alias("cost_bucket"),
    )
)
