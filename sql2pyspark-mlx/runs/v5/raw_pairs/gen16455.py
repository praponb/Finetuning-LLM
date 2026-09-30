from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("sqft_rounded", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

valid = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.round("sqft", 1).alias("sqft_rounded"),
        F.coalesce("region", F.lit("DIRECT")).alias("region_filled"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") < F.col("s.opened_date")), "left_anti")
)

with_windows = (
    eligible
    .filter(F.year("opened_date") == 2023)
    .select(
        "id",
        "store_name",
        "region",
        "sqft_rounded",
        F.count("sqft").over(w_grp).alias("running_count_sqft"),
        F.max("sqft").over(w_grp).alias("running_max_sqft"),
        F.min("sqft").over(w_grp).alias("running_min_sqft"),
        F.avg("sqft").over(w2).alias("sqft_avg_6_prior"),
    )
)

kept = (
    with_windows
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.store_id").isin([1, 2, 3, 5])), "left_anti")
)

with_last = (
    kept
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.region",
        "s.sqft_rounded",
        "s.running_min_sqft",
        F.col("c.last_restock"),
    )
)

result = (
    with_last
    .groupBy(F.date_trunc("WEEK", "last_restock").alias("week_start"))
    .agg(
        F.sum("sqft_rounded").alias("total_sqft_rounded"),
        F.round(F.sum("sqft_rounded") / F.nullif(F.count(F.when(F.col("region") == "East", 1)), F.lit(0)), 2).alias("sqft_rounded_per_east"),
    )
    .orderBy(F.col("total_sqft_rounded").desc())
)
