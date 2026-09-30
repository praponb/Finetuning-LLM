from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost")
w2 = Window.partitionBy("status", "order_id").orderBy(F.col("cost").desc_nulls_last())
w3 = Window.partitionBy("order_id").orderBy(F.col("cost_dense_rank_2").desc_nulls_last(), "id")

valid = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .withColumn("status_title", F.initcap(F.trim("status")))
)

scored = (
    valid
    .select(
        "*",
        F.dense_rank().over(w).alias("cost_dense_rank"),
        F.dense_rank().over(w2).alias("cost_dense_rank_2"),
    )
)

joined = (
    scored
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id").eqNullSafe(F.col("o.id")), "inner")
    .filter(F.col("o.shipping_fee").between(33, 38))
    .select("a.*", F.col("o.amount"), F.col("o.status").alias("order_status"))
)

deduped = (
    joined
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .groupBy("order_status")
    .agg(
        F.stddev_samp("cost").alias("cost_stddev"),
        F.count(F.when(F.col("order_status") == "SHIPPED", 1)).alias("shipped_rows"),
    )
)
