from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("order_id")
w_hist = Window.partitionBy("carrier", "order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("carrier")
w_recent = Window.partitionBy("carrier", "order_id").orderBy("cost", "id")

filtered = (
    df_shipments
    .filter(~F.col("carrier").eqNullSafe(F.lit("Flash")))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.when(F.col("carrier") == "DHL", 1).otherwise(0).alias("is_dhl"),
    )
)

result = (
    filtered
    .filter(F.col("order_id").isin([3, 11, 14]))
    .withColumn("order_id_count_is_dhl", F.count("is_dhl").over(w_ordered))
    .withColumn("cumulative_is_dhl", F.sum("is_dhl").over(w_hist))
    .withColumn("running_is_dhl", F.sum("is_dhl").over(w_part))
    .withColumn("carrier_stddev_samp_is_dhl", F.stddev_samp("is_dhl").over(w4))
    .withColumn("cost_bucket", F.ntile(4).over(w_recent))
)
