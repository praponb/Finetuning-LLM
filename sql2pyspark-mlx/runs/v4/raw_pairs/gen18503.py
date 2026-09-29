from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier").orderBy("shipped_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("cost", "id")
w5 = Window.partitionBy("order_id")
w6 = Window.partitionBy("order_id").orderBy("first_known_cost_2", "id")
w7 = Window.partitionBy("carrier").orderBy("first_known_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("first_known_cost_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("order_id").orderBy(F.col("row_seq").desc())
w10 = Window.partitionBy("carrier", "order_id").orderBy("first_known_cost_2_bucket", "id").rowsBetween(-2, -1)
w11 = Window.partitionBy("status").orderBy("first_known_cost_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_shipments
    .withColumn("cost_avg_2", F.avg("cost").over(w))
    .withColumn("first_known_cost", F.first("cost", ignorenulls=True).over(w2))
    .withColumn("last_known_carrier", F.last("carrier", ignorenulls=True).over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("first_known_cost_2", F.first("cost", ignorenulls=True).over(w3))
)

scored_shipments = (
    scored
    .withColumn("order_id_stddev_samp_cost_avg_2", F.stddev_samp("cost_avg_2").over(w5))
    .withColumn("running_row_seq", F.sum("row_seq").over(w3))
    .withColumn("first_known_cost_2_bucket", F.ntile(4).over(w6))
    .withColumn("last_known_row_seq", F.last("row_seq", ignorenulls=True).over(w7))
    .withColumn("first_known_cost_avg_2", F.first("cost_avg_2", ignorenulls=True).over(w_part))
)

scored_shipments_2 = (
    scored_shipments
    .withColumn("row_seq_dense_rank", F.dense_rank().over(w9))
    .withColumn("last_known_row_seq_avg_2_prior", F.avg("last_known_row_seq").over(w10))
    .withColumn("running_first_known_cost_2_bucket", F.sum("first_known_cost_2_bucket").over(w11))
)

result = (
    scored_shipments_2
    .select(
        "id",
        "carrier",
        "delivered_date",
        "status",
        "cost_avg_2",
        "first_known_cost",
        "order_id_stddev_samp_cost_avg_2",
        "running_row_seq",
        "first_known_cost_2_bucket",
    )
    .orderBy(
        F.col("order_id_stddev_samp_cost_avg_2").desc_nulls_last(),
        F.col("delivered_date").desc(),
    )
)
