from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("carrier")
w3 = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("order_id", "status").orderBy(F.col("carrier_sum_cost").desc(), "id")
w5 = Window.partitionBy("order_id")

filtered = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .withColumn("status_upper", F.upper("status"))
    .withColumn("carrier_clean", F.coalesce("carrier", F.lit("OTHER")))
)

scored = (
    filtered
    .withColumn("first_known_cost", F.first("cost", ignorenulls=True).over(w))
    .withColumn("carrier_sum_cost", F.sum("cost").over(w_ordered))
    .withColumn("cost_avg_2_prior", F.avg("cost").over(w3))
)

valid = (
    scored
    .filter((F.col("delivered_date") > F.lit("2023-08-17").cast("date")) & F.col("carrier").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "status_upper",
        "first_known_cost",
        "carrier_sum_cost",
        "cost_avg_2_prior",
        (F.col("carrier_sum_cost") * F.col("cost")).alias("carrier_sum_cost_times_cost"),
    )
)

ranked = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

scored_shipments = (
    ranked
    .select(
        "*",
        F.max("cost_avg_2_prior").over(w5).alias("order_id_max_cost_avg_2_prior"),
        F.stddev_samp("cost").over(w_ordered).alias("carrier_stddev_samp_cost"),
    )
)

with_dims = (
    scored_shipments
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select("s.*", F.col("o.status").alias("order_status"))
)

result = (
    with_dims
    .groupBy("status", "order_status")
    .pivot("carrier", ["Flash", "FedEx", "UPS"])
    .agg(F.sum("cost"))
)
