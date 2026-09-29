from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue").desc())
w2 = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")
w3 = Window.partitionBy("store_id")
w4 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("row_seq", "id").rowsBetween(-5, -1)
w6 = Window.partitionBy("store_id").orderBy(F.col("revenue_cume_dist").desc(), "id")

enriched = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "sale_date",
        F.cume_dist().over(w).alias("revenue_cume_dist"),
        F.row_number().over(w2).alias("row_seq"),
        F.corr("revenue", "units").over(w3).alias("corr_revenue_units"),
    )
)

with_windows = (
    enriched
    .filter(F.col("store_id").isin([1, 2, 6]))
    .withColumn("cumulative_row_seq", F.sum("row_seq").over(w4))
    .withColumn("row_seq_avg_5_prior", F.avg("row_seq").over(w5))
    .withColumn("revenue_cume_dist_bucket", F.ntile(5).over(w6))
)

remaining_keys = (
    with_windows
    .select("id", "store_id")
    .subtract(
        df_store_sales
        .filter(F.col("revenue").between(1100, 2600))
        .select("id", "store_id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.region"), F.col("b.opened_date"), F.col("b.city"))
)

joined = (
    with_dims
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.city").alias("store_city"))
)

result = (
    joined
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue_cume_dist",
        "row_seq",
        "corr_revenue_units",
        "row_seq_avg_5_prior",
        "revenue_cume_dist_bucket",
        "region",
        F.when(F.col("region").rlike("^(West|East)$"), "GROUP_B").when(F.col("region").rlike("^(North|South)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("region_zone"),
    )
)
