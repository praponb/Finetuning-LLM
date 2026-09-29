from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id")

deduped = (
    df_orders
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
    )
)

result = (
    deduped
    .withColumn("activity_month", F.date_trunc("month", "order_date"))
    .cube("status", "activity_month")
    .agg(
        F.percentile_approx("shipping_fee", [0.25, 0.5, 0.75]).alias("shipping_fee_quartiles"),
        F.stddev_samp("shipping_fee").alias("shipping_fee_stddev"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.grouping("status").alias("grouping_id"),
    )
    .orderBy(F.col("status").desc())
)
