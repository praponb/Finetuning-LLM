from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.orderBy("revenue")

joined = (
    df_store_sales
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.sqft"), F.col("p.region"), F.col("p.store_name"))
)

matched_keys = (
    joined
    .select("id")
    .intersectAll(
        df_store_sales
        .filter(F.col("revenue").between(2900, 3900))
        .select("id")
    )
)

reconciled = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .filter(F.col("revenue") < 2200)
    .select(
        "*",
        F.corr("units", "revenue").over(w).alias("corr_units_revenue"),
        F.round((F.col("revenue") / F.sum("revenue").over(w)), 4).alias("revenue_share"),
    )
)

scored = (
    metrics
    .select(
        "store_id",
        "sqft",
        "store_name",
        "revenue_share",
        F.percent_rank().over(w2).alias("revenue_percent_rank"),
    )
)

result = (
    scored
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "inner")
    .rollup(F.col("d.region").alias("region"))
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("a.sqft")), 2).alias("avg_sqft"),
        F.round(F.avg(F.col("a.revenue_percent_rank")), 2).alias("avg_revenue_percent_rank"),
    )
    .orderBy(F.col("rows_total").asc_nulls_last())
)
