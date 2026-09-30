from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("order_id", "status")
w4 = Window.orderBy(F.col("cost_avg_6_prior").asc_nulls_first())

enriched = (
    df_shipments
    .withColumn("last_known_cost", F.last("cost", ignorenulls=True).over(w))
    .withColumn("cost_avg_6_prior", F.avg("cost").over(w_recent))
)

kept_keys = (
    enriched
    .select("order_id", "id")
    .subtract(
        df_shipments
        .filter(F.col("order_id") == 14)
        .select("order_id", "id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "*",
        F.sum("last_known_cost").over(w3).alias("order_id_sum_last_known_cost"),
        F.rank().over(w4).alias("cost_avg_6_prior_rank"),
    )
)

result = (
    with_windows
    .groupBy("order_id")
    .pivot("status", ["LOST", "IN_TRANSIT", "RETURNED"])
    .agg(F.max("cost_avg_6_prior"))
    .select(
        "order_id",
        F.col("LOST").alias("lost"),
        F.col("IN_TRANSIT").alias("in_transit"),
        F.col("RETURNED").alias("returned"),
    )
)
