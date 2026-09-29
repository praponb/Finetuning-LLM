from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("store_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_orders
    .filter(F.col("discount") > 0.3)
    .withColumn("running_avg_shipping_fee", F.avg("shipping_fee").over(w_grp))
    .withColumn("running_max_shipping_fee", F.max("shipping_fee").over(w_grp))
    .withColumn("running_min_shipping_fee", F.min("shipping_fee").over(w_grp))
)

result = (
    scored
    .groupBy(
        F.when(F.col("shipping_fee") >= 32, "LARGE").otherwise("SMALL").alias("shipping_fee_tier"),
        F.when(F.col("amount") >= 1220, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
        "status",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("shipping_fee", 0.5).alias("shipping_fee_p50"),
        F.round(F.avg("running_max_shipping_fee"), 2).alias("avg_running_max_shipping_fee"),
        F.min("running_max_shipping_fee").alias("min_running_max_shipping_fee"),
    )
    .orderBy(F.col("shipping_fee_tier").desc())
)
