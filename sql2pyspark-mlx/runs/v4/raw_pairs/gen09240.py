from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("sqft_tier").orderBy(F.col("sqft_bit_and").desc())

with_latest = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.discount").alias("order_discount"))
)

result = (
    with_latest
    .groupBy(
        F.when(F.col("sqft") >= 16607, "HIGH").otherwise("LOW").alias("sqft_tier"),
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
    )
    .agg(
        F.round(F.avg("order_discount"), 2).alias("avg_order_discount"),
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.avg("order_discount").alias("avg_order_discount_2"),
    )
    .withColumn("sqft_bit_and_dense_rank", F.dense_rank().over(w2))
    .orderBy(F.col("sqft_tier").desc_nulls_first())
)
