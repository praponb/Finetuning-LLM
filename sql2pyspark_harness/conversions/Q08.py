"""Q08 — As-of FX join + RANGE/ROWS window frames (reference conversion).

Traps:
  * RANGE BETWEEN INTERVAL 30 DAYS PRECEDING needs a numeric ordering in the DataFrame
    API: order by the timestamp cast to epoch seconds and use rangeBetween(-30*86400, 0).
  * LAST_VALUE(x) IGNORE NULLS  ->  F.last(x, ignorenulls=True)
  * /*+ BROADCAST(f) */          ->  F.broadcast(df)
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

DAY = 86400


def build(spark: SparkSession) -> DataFrame:
    o, oi, fx = spark.table("orders"), spark.table("order_items"), spark.table("fx_rates")

    order_totals = (
        o.join(oi, "order_id")
         .groupBy("order_id", "customer_id", "order_ts", "currency", "status", "channel")
         .agg(F.sum(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct"))).alias("net_local"))
    )

    f = F.broadcast(fx.alias("f"))
    ot = order_totals.alias("ot")
    fx_pit = (
        ot.join(f, (F.col("f.currency") == F.col("ot.currency"))
                   & (F.col("f.rate_date") <= F.to_date(F.col("ot.order_ts"))))
          .select(
              F.col("ot.order_id").alias("order_id"),
              F.col("f.rate_to_usd").alias("rate_to_usd"),
              F.col("f.rate_date").alias("rate_date"),
              F.row_number().over(Window.partitionBy("ot.order_id").orderBy(F.col("f.rate_date").desc())).alias("rn"),
          )
          .where("rn = 1")
    )

    usd = (
        order_totals.join(fx_pit, "order_id", "left")
        .select(
            *order_totals.columns,
            F.col("rate_date").alias("fx_rate_date"),
            (F.col("net_local") * F.col("rate_to_usd")).cast("decimal(18,2)").alias("net_usd"),
        )
    )

    ts_secs = F.col("order_ts").cast("long")
    by_cust = Window.partitionBy("customer_id").orderBy("order_ts")

    return (
        usd.select(
            "order_id", "customer_id", "order_ts", "currency", "fx_rate_date", "status", "net_usd",
            F.sum("net_usd").over(Window.orderBy(ts_secs).rangeBetween(-30 * DAY, 0)).alias("rolling_30d_usd"),
            F.count(F.lit(1)).over(
                Window.partitionBy("customer_id").orderBy(ts_secs).rangeBetween(-60 * DAY, 0)
            ).alias("cust_orders_60d"),
            F.sum("net_usd").over(by_cust.rowsBetween(Window.unboundedPreceding, Window.currentRow))
             .alias("cust_running_usd"),
            F.avg("net_usd").over(Window.partitionBy("channel").orderBy("order_ts").rowsBetween(-2, 2))
             .alias("channel_centered_avg5"),
            F.last(F.when(F.col("status") == "DELIVERED", F.col("order_id")), ignorenulls=True)
             .over(by_cust.rowsBetween(Window.unboundedPreceding, -1)).alias("prev_delivered_order"),
            F.first("order_ts").over(by_cust).alias("first_order_ts"),
            F.datediff(F.to_date("order_ts"), F.to_date(F.lag("order_ts").over(by_cust))).alias("days_since_prev"),
            F.lead("status", 1, "NONE").over(by_cust).alias("next_status"),
            F.dense_rank().over(
                Window.partitionBy(F.date_trunc("MONTH", "order_ts")).orderBy(F.col("net_usd").desc())
            ).alias("month_rank"),
        )
        .orderBy("order_ts")
    )
