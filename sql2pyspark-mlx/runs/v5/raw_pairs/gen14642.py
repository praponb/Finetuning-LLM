from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy("cost", "id")
w2 = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(-2, -1)

result = (
    df_shipments
    .filter(F.col("delivered_date") > "2023-11-01")
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.avg("cost").over(w2).alias("cost_avg_2_prior"),
    )
)
