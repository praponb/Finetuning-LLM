from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date_period", "id")
w2 = Window.partitionBy("store_id").orderBy("sale_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id", "product_id").orderBy(F.col("sale_date_period").desc_nulls_first(), "id")
w4 = Window.orderBy("units", "id")
w5 = Window.orderBy(F.col("units").desc())

base = (
    df_store_sales
    .filter(F.col("units").isNull())
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.date_trunc("WEEK", "sale_date").alias("sale_date_period"),
        F.round((F.col("units") / F.col("revenue")), 2).alias("revenue_per_unit"),
    )
)

gaps = (
    base
    .withColumn("is_new_burst", F.when(F.lag("sale_date_period").over(w).isNull() | (F.col("sale_date_period") > F.lag("sale_date_period").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

ranked = (
    islands
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "sale_date", "units", "revenue", "revenue_per_unit", "is_new_burst")
)

with_windows = (
    ranked
    .filter(F.col("revenue") <= 2400)
    .select(
        "*",
        F.lag("is_new_burst").over(w4).alias("prev_is_new_burst"),
        F.lead("is_new_burst").over(w4).alias("next_is_new_burst"),
        F.lag("units").over(w4).alias("prev_units"),
        F.lead("units").over(w4).alias("next_units"),
        F.row_number().over(w4).alias("event_seq"),
        F.dense_rank().over(w5).alias("units_dense_rank"),
    )
)

result = (
    with_windows
    .select("id", "revenue", "revenue_per_unit")
    .unpivot(
        ids=["id"],
        values=["revenue", "revenue_per_unit"],
        variableColumnName="kpi",
        valueColumnName="metric_value",
    )
)
