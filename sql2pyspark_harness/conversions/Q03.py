"""Q03 — PIVOT with multiple aggregates and aliased pivot values (reference conversion)."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# SQL: FOR order_month IN ('2024-01' AS jan, ...). The DataFrame pivot() has no alias
# list, so columns come out as "2024-01_rev" and must be renamed to "jan_rev".
MONTHS = {
    "2024-01": "jan", "2024-02": "feb", "2024-03": "mar", "2024-04": "apr",
    "2024-05": "may", "2024-06": "jun", "2024-07": "jul",
}


def build(spark: SparkSession) -> DataFrame:
    oi, o, p = spark.table("order_items"), spark.table("orders"), spark.table("products")

    src = (
        oi.join(o, "order_id")
          .join(p, "product_id")
          .where(F.col("status").isin("DELIVERED", "SHIPPED"))
          .select(
              "category",
              F.date_format("order_ts", "yyyy-MM").alias("order_month"),
              (F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct"))).alias("net_local"),
              F.col("quantity").alias("units"),
          )
    )

    pivoted = (
        src.groupBy("category")
           .pivot("order_month", list(MONTHS))
           .agg(F.round(F.sum("net_local"), 2).alias("rev"), F.sum("units").alias("units"))
    )

    renamed = pivoted
    for value, alias in MONTHS.items():
        for agg in ("rev", "units"):
            renamed = renamed.withColumnRenamed(f"{value}_{agg}", f"{alias}_{agg}")

    return renamed.orderBy("category")
