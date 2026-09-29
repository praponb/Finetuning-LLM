from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("shipped_date", "id")
w3 = Window.partitionBy("order_id", "status")
w4 = Window.partitionBy("status", "order_id").orderBy(F.col("cost").desc_nulls_first())
w5 = Window.partitionBy("carrier").orderBy("first_known_cost", "id")
w6 = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("carrier").orderBy("order_id_sum_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status").orderBy("first_known_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_shipments
    .filter((F.col("delivered_date") > F.to_date(F.lit("2023-06-17"))) & F.col("carrier").isNotNull())
    .select(
        "*",
        F.lower("carrier").alias("carrier_lower"),
        F.when(F.col("carrier").isin(["DHL", "Flash"]), "Tier 1").otherwise("Other").alias("carrier_group"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.first("cost", ignorenulls=True).over(w).alias("first_known_cost"),
        F.lag("cost", 2).over(w2).alias("prev_cost"),
        F.sum("cost").over(w3).alias("order_id_sum_cost"),
    )
)

metrics = (
    enriched
    .withColumn("cost_dense_rank", F.dense_rank().over(w4))
    .withColumn("row_seq", F.row_number().over(w5))
    .withColumn("cumulative_prev_cost", F.sum("prev_cost").over(w6))
    .withColumn("cumulative_prev_cost_2", F.sum("prev_cost").over(w7))
    .withColumn("cumulative_prev_cost_3", F.sum("prev_cost").over(w8))
)

valid = (
    metrics
    .filter(F.col("shipped_date") > F.lit("2024-02-04").cast("date"))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "status",
        "carrier_lower",
        "carrier_group",
        "first_known_cost",
        "prev_cost",
        "order_id_sum_cost",
        "cost_dense_rank",
        "row_seq",
        "cumulative_prev_cost",
        "cumulative_prev_cost_3",
        F.col("cumulative_prev_cost").cast("double").alias("cumulative_prev_cost_num"),
        F.greatest("cumulative_prev_cost_2", "first_known_cost").alias("greatest_cumulative_prev_cost_2_first_known_cost"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("resolved_carrier"),
    )
)

result = (
    valid
    .groupBy("order_id")
    .pivot("status", ["IN_TRANSIT", "DELIVERED"])
    .agg(F.avg("first_known_cost").alias("total"))
)
