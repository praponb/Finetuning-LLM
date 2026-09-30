from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("carrier").orderBy("cost", "id").rowsBetween(-2, -1)
w_recent_2 = Window.partitionBy("order_id")

result = (
    df_shipments
    .select(
        "id",
        "shipped_date",
        "status",
        F.avg("cost").over(w_recent).alias("cost_avg_2_prior"),
        F.count("cost").over(w_recent_2).alias("order_id_count_cost"),
    )
)
