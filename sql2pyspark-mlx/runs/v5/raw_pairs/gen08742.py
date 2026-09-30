from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city", "region").orderBy(F.col("sqft").desc(), "id")
w_grp = Window.partitionBy("city").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("region").orderBy(F.col("sqft").asc_nulls_last())

with_latest = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount"))
)

kept = (
    with_latest
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.opened_date")), "left_semi")
)

latest = (
    kept
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

not_flagged = (
    latest
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.store_id").isin([1, 4, 5, 6])), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "store_name",
        "opened_date",
        "amount",
        F.sum("sqft").over(w_grp).alias("cumulative_sqft"),
        F.percent_rank().over(w4).alias("sqft_percent_rank"),
        F.sum("amount").over(w_grp).alias("cumulative_amount"),
    )
)
