from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost_int", "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("cost").desc(), "id")
w_recent = Window.partitionBy("carrier").orderBy(F.col("cost_int").desc())

filtered = (
    df_shipments
    .filter(F.col("shipped_date") > F.lit("2023-12-08").cast("date"))
    .withColumn("status_prefix", F.substring("status", 1, 3))
    .withColumn("cost_int", F.col("cost").cast("int"))
    .withColumn("carrier_clean", F.coalesce("carrier", F.lit("DIRECT")))
)

result = (
    filtered
    .select(
        "*",
        F.ntile(4).over(w).alias("cost_int_bucket"),
        F.ntile(5).over(w2).alias("cost_bucket"),
        F.dense_rank().over(w_recent).alias("cost_int_dense_rank"),
    )
)
