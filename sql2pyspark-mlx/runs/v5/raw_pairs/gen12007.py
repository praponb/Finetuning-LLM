from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date_period", "id")
w2 = Window.partitionBy("product_id").orderBy("sale_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("sale_date_period", "id")
w4 = Window.partitionBy("store_id").orderBy("sale_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("revenue", "id")
w6 = Window.partitionBy("store_id").orderBy(F.col("session_no").desc())
w7 = Window.partitionBy("store_id").orderBy("session_no_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("revenue_per_unit", "id")

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-02-19").cast("date"))
    .withColumn("revenue_per_unit", (F.col("revenue") + F.col("units")))
)

filtered = (
    cleaned
    .filter(F.col("sale_date") > F.lit("2023-06-04").cast("date"))
    .select("*", F.date_trunc("year", "sale_date").alias("sale_date_period"))
)

with_gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.col("sale_date_period") - F.lag("sale_date_period").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "revenue_per_unit",
        "sale_date_period",
        F.sum("is_new_session").over(w2).alias("session_id"),
    )
)

flagged = (
    islands
    .withColumn("new_group_flag", F.when(F.lag("sale_date_period").over(w3).isNull() | (F.col("sale_date_period") > F.lag("sale_date_period").over(w3) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "store_id",
        "revenue",
        "revenue_per_unit",
        "sale_date_period",
        "session_id",
        F.sum("new_group_flag").over(w4).alias("session_no"),
    )
)

scored = (
    sessionized
    .filter(F.col("sale_date_period") >= "2024-10-01")
    .select(
        "*",
        F.lag("revenue_per_unit").over(w5).alias("prev_revenue_per_unit"),
        F.lead("revenue_per_unit").over(w5).alias("next_revenue_per_unit"),
        F.lead("session_id").over(w5).alias("next_session_id"),
        F.row_number().over(w5).alias("event_seq"),
        F.cume_dist().over(w6).alias("session_no_cume_dist"),
    )
)

result = (
    scored
    .select(
        "id",
        "revenue",
        "session_id",
        "session_no",
        "next_session_id",
        "event_seq",
        F.last("revenue_per_unit", ignorenulls=True).over(w7).alias("last_known_revenue_per_unit"),
        F.row_number().over(w8).alias("seq_no"),
        F.lead("revenue_per_unit", 1, 0).over(w5).alias("next_revenue_per_unit_2"),
    )
)
