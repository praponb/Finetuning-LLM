from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "carrier").orderBy(F.col("cost").desc(), "id")
w2 = Window.orderBy(F.col("cost").desc_nulls_last())
w3 = Window.partitionBy("status", "order_id").orderBy("cost", "id")

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "cost",
        F.row_number().over(w).alias("row_seq"),
        F.rank().over(w2).alias("cost_rank"),
        F.lead("cost", 1).over(w3).alias("next_cost"),
    )
)
