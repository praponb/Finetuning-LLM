from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("shipped_date_period", "id")
w2 = Window.partitionBy("status", "order_id").orderBy(F.col("cost").desc(), "id")

valid = (
    df_shipments
    .filter(F.col("delivered_date").between("2023-11-25", "2025-09-12"))
    .select("*", F.date_trunc("MONTH", "shipped_date").alias("shipped_date_period"))
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

deduped = (
    latest
    .withColumn("rank_in_group_2", F.row_number().over(w2))
    .filter(F.col("rank_in_group_2") == 1)
)

with_dims = (
    deduped
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.status").alias("order_status"))
)

remaining_keys = (
    with_dims
    .select("status", "carrier")
    .subtract(
        df_shipments
        .filter(F.col("delivered_date") < "2025-05-01")
        .select("status", "carrier")
    )
)

reconciled = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter((F.col("delivered_date") > F.lit("2023-08-15").cast("date")) & F.col("order_status").isNotNull())
    .select(
        "*",
        (F.col("rank_in_group") - F.col("cost")).alias("rank_in_group_minus_cost"),
    )
)

cleaned_shipments = (
    cleaned
    .filter((F.col("delivered_date") > F.to_date(F.lit("2023-12-03"))) & (F.col("delivered_date") > "2023-04-01"))
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        "shipped_date_period",
        "rank_in_group",
        "rank_in_group_2",
        "order_status",
        "rank_in_group_minus_cost",
        F.date_trunc("YEAR", "delivered_date").alias("delivered_date_period"),
    )
)

totals = (
    cleaned_shipments
    .groupBy("order_status")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.percentile_approx("cost", 0.95).alias("cost_p95"),
        ((F.unix_timestamp(F.max("delivered_date_period")) - F.unix_timestamp(F.min("delivered_date_period"))) / 60).alias("duration_min"),
        F.sum(F.when(F.col("carrier") == "FedEx", F.col("rank_in_group_minus_cost"))).alias("fedex_total_rank_in_group_minus_cost"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    totals
    .select(
        "order_status",
        "status_list",
        "duration_min",
        "distinct_ids",
        F.when(F.col("order_status").rlike("^(PENDING|COMPLETED)$"), "GROUP_A").when(F.col("order_status").rlike("^(CANCELLED|SHIPPED)$"), "EXTENDED").otherwise("GROUP_B").alias("order_status_zone"),
    )
    .orderBy(F.col("order_status").asc_nulls_first(), "duration_min")
)
