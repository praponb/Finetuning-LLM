from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipping_fee").desc(), "id")
w_grp = Window.partitionBy("status").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("carrier").orderBy("cost", "id")
w4 = Window.partitionBy("carrier").orderBy(F.col("order_date").desc(), "id")
w5 = Window.partitionBy("order_id").orderBy("shipping_fee", "id")
w6 = Window.orderBy("first_known_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined = (
    df_shipments
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .select(
        "src.id",
        "src.order_id",
        "src.carrier",
        "src.cost",
        "src.status",
        F.col("o.shipping_fee"),
        F.col("o.order_date"),
    )
)

metrics = (
    joined
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.first("cost", ignorenulls=True).over(w_grp).alias("first_known_cost"),
        F.ntile(10).over(w3).alias("cost_bucket"),
    )
)

deduped = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    deduped
    .select(
        "id",
        "carrier",
        "status",
        "order_date",
        "row_seq",
        "first_known_cost",
        "cost_bucket",
        F.lead("shipping_fee", 1).over(w5).alias("next_shipping_fee"),
        F.sum("cost").over(w6).alias("cumulative_cost"),
    )
)

result = (
    enriched
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("carrier") == "DHL", F.col("cumulative_cost")).otherwise(0)).alias("dhl_cumulative_cost"),
        F.round(F.avg("cumulative_cost"), 2).alias("avg_cumulative_cost"),
        F.sum(F.when(F.col("status") == "RETURNED", F.col("row_seq")).otherwise(0)).alias("returned_row_seq"),
        F.percentile_approx("cost_bucket", [0.25, 0.5, 0.75]).alias("cost_bucket_quartiles"),
        F.sum(F.when(F.col("carrier") == "UPS", F.col("row_seq")).otherwise(0)).alias("ups_row_seq"),
    )
    .filter(F.col("dhl_cumulative_cost") > 0)
    .orderBy(F.col("avg_cumulative_cost").desc())
)
