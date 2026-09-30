from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("cost")
w_recent = Window.orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("carrier").orderBy(F.col("cost").desc_nulls_first())

with_windows = (
    df_shipments
    .select(
        "*",
        F.last("carrier", ignorenulls=True).over(w).alias("last_known_carrier"),
        F.rank().over(w_grp).alias("cost_rank"),
        F.last("carrier", ignorenulls=True).over(w_recent).alias("last_known_carrier_2"),
        F.dense_rank().over(w4).alias("cost_dense_rank"),
    )
)

result = (
    with_windows
    .groupBy(
        F.date_trunc("WEEK", "shipped_date").alias("week_start"),
        F.date_trunc("QUARTER", "delivered_date").alias("quarter_start"),
        F.date_trunc("YEAR", "shipped_date").alias("year_start"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
    .orderBy(F.col("returned_count").asc_nulls_first())
)
