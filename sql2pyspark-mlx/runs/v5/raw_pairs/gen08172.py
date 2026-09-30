from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status")
w3 = Window.orderBy(F.col("order_date").desc(), "id")

enriched = (
    df_orders
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        F.first("amount", ignorenulls=True).over(w_recent).alias("first_known_amount"),
        F.corr("amount", "discount").over(w2).alias("corr_amount_discount"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

result = (
    enriched
    .withColumn("first_known_amount_tier", F.when(F.col("first_known_amount") >= 1550, "PREMIUM").otherwise("STANDARD"))
    .withColumn("discount_tier", F.when(F.col("discount") >= 0.36, "PREMIUM").otherwise("STANDARD"))
    .rollup("first_known_amount_tier", "discount_tier")
    .agg(
        F.percentile_approx("corr_amount_discount", [0.25, 0.5, 0.75]).alias("corr_amount_discount_quartiles"),
        F.countDistinct("status").alias("distinct_status"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_rows"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.count("*").alias("row_count"),
        F.grouping("first_known_amount_tier").alias("gid"),
    )
)
