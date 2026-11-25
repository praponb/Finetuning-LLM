"""Q07 — Set operations (reference conversion).

Traps this query is designed to catch:
  * DataFrame.union()  == SQL UNION ALL (no dedup). SQL UNION == union().distinct().
  * SQL INTERSECT / EXCEPT are DISTINCT: use intersect() / subtract(), not the *All variants.
  * Set ops are left-associative: A UNION ALL B UNION ALL C UNION ALL D UNION E
    dedups the WHOLE result, not just the last pair.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build(spark: SparkSession) -> DataFrame:
    orders, events = spark.table("orders"), spark.table("web_events")

    web_buyers = orders.where(F.col("channel") == "web").select("customer_id").distinct()
    app_buyers = orders.where(F.col("channel") == "app").select("customer_id").distinct()
    browsers = events.where(F.col("customer_id").isNotNull()).select("customer_id").distinct()
    kept_buyers = orders.where(F.col("status").isin("DELIVERED", "SHIPPED")).select("customer_id")

    def tag(df: DataFrame, cohort: str) -> DataFrame:
        return df.select(F.lit(cohort).alias("cohort"), "customer_id")

    return (
        tag(web_buyers.intersect(app_buyers), "web_and_app")
        .unionAll(tag(web_buyers.subtract(app_buyers), "web_only"))
        .unionAll(tag(app_buyers.subtract(web_buyers), "app_only"))
        .unionAll(tag(browsers.subtract(kept_buyers), "browsed_no_purchase"))
        .union(tag(browsers.unionAll(orders.select("customer_id")), "any_activity"))
        .distinct()
        .orderBy("cohort", "customer_id")
    )
