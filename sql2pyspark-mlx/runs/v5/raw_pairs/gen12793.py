from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("carrier")
w3 = Window.partitionBy("status").orderBy(F.col("carrier_min_cost").desc_nulls_last(), "id")
w4 = Window.partitionBy("status", "order_id").orderBy("cost_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status", "order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_shipments
    .withColumn("cost_bucket", F.ntile(5).over(w))
    .withColumn("carrier_min_cost", F.min("cost").over(w2))
)

deduped = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

joined = (
    deduped
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.status").alias("order_status"))
)

scored = (
    joined
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "status",
        "cost_bucket",
        "order_status",
        F.sum("carrier_min_cost").over(w4).alias("running_carrier_min_cost"),
        F.last("order_status", ignorenulls=True).over(w5).alias("last_known_order_status"),
    )
)

valid = (
    scored
    .filter(F.col("order_status").isNotNull() & (~F.col("order_status").eqNullSafe(F.lit("RETURNED"))))
    .select(
        "*",
        F.last_day("delivered_date").alias("delivered_date_month_end"),
        F.coalesce("order_status", F.lit("N/A")).alias("resolved_order_status"),
    )
)

result = (
    valid
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "status",
        "running_carrier_min_cost",
        "last_known_order_status",
        "delivered_date_month_end",
        "resolved_order_status",
        F.when(F.col("status").rlike("^(DELIVERED|IN_TRANSIT)$"), "GROUP_A").when(F.col("status").rlike("^(LOST|RETURNED)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("status_zone"),
    )
    .orderBy(F.col("delivered_date_month_end").desc_nulls_last(), "shipped_date")
)
