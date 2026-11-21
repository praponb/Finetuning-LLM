"""Q04a — GROUPING SETS + grouping()/grouping_id() + FILTER clause (reference conversion).

Requires Spark 4.0+ for DataFrame.groupingSets().
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build(spark: SparkSession) -> DataFrame:
    o, c = spark.table("orders"), spark.table("customers")
    oi, p = spark.table("order_items"), spark.table("products")

    base = (
        o.join(c, "customer_id")
         .join(oi, "order_id")
         .join(p, "product_id")
         .select(
             "country_code", "channel", "category", "status", "quantity",
             (F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct"))).alias("net_local"),
         )
    )

    cc, ch, cat, status = F.col("country_code"), F.col("channel"), F.col("category"), F.col("status")

    # groupingSets(...).agg() returns the grouping columns followed by the aggregates.
    # grouping() must be computed inside agg(), so capture the flags and label afterwards.
    # FILTER (WHERE cond) on an aggregate == aggregate over CASE WHEN cond THEN x END.
    return (
        base.groupingSets([[cc, ch, cat], [cc, ch], [cc], [ch, cat], []], cc, ch, cat)
        .agg(
            F.grouping(cc).alias("_g_cc"),
            F.grouping(ch).alias("_g_ch"),
            F.grouping(cat).alias("_g_cat"),
            F.grouping_id(cc, ch, cat).alias("gid"),
            F.count(F.lit(1)).alias("lines"),
            F.sum("quantity").alias("units"),
            F.sum(F.when(status == "DELIVERED", F.col("quantity"))).alias("units_delivered"),
            F.count(F.when(status.isin("CANCELLED", "RETURNED"), F.lit(1))).alias("lines_lost"),
            F.round(F.sum(F.when(status != "CANCELLED", F.col("net_local"))), 2).alias("net_local_ex_cancel"),
        )
        .select(
            F.when(F.col("_g_cc") == 1, "(all countries)").otherwise(cc).alias("country_code"),
            F.when(F.col("_g_ch") == 1, "(all channels)").otherwise(ch).alias("channel"),
            F.when(F.col("_g_cat") == 1, "(all categories)").otherwise(cat).alias("category"),
            "gid", "lines", "units", "units_delivered", "lines_lost", "net_local_ex_cancel",
        )
        .orderBy("gid", "country_code", "channel", "category")
    )
