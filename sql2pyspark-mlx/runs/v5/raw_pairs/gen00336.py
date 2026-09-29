from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "carrier").orderBy("cost")
w_running = Window.orderBy("cost_whole", "id")
w3 = Window.partitionBy("carrier", "status").orderBy("cost", "id")

valid = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-06-14").cast("date")) & F.col("shipped_date").between("2023-06-19", "2025-03-21"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.ceil("cost").alias("cost_whole"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_clean"),
    )
)

result = (
    valid
    .select(
        "id",
        "cost_whole",
        F.dense_rank().over(w).alias("cost_dense_rank"),
        F.row_number().over(w_running).alias("row_seq"),
        F.ntile(3).over(w3).alias("cost_bucket"),
    )
)
