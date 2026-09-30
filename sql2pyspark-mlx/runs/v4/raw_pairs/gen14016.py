from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "customer_id").orderBy(F.col("shipping_fee").desc_nulls_first(), "id")

deduped = (
    df_orders
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .withColumn("week_start", F.date_trunc("WEEK", "order_date"))
    .groupingSets(
        [["store_id", "week_start", "channel"], ["store_id", "week_start"]],
        "store_id",
        "week_start",
        "channel",
    )
    .agg(
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("amount_per_store"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_rows"),
        F.stddev_samp("discount").alias("discount_stddev"),
        F.size(F.collect_set("channel")).alias("num_channels"),
    )
    .orderBy(F.col("channel").desc_nulls_first())
)
