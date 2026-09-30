from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc_nulls_first(), "id")

valid = (
    df_store_sales
    .filter(F.col("store_id") == 6)
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.round("revenue", 0).alias("revenue_rounded"),
    )
)

deduped = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_dims = (
    deduped
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.launch_date") <= "2023-04-05")
    .select("s.*", F.col("p.launch_date"))
)

joined = (
    with_dims
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.product_id",
        "a.units",
        "a.revenue_rounded",
        "a.launch_date",
        F.col("d.product_name"),
        F.col("d.stock_qty"),
    )
)

stats = (
    joined
    .groupBy(F.date_trunc("QUARTER", "launch_date").alias("quarter_start"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.count("*").alias("cnt"),
        F.bit_or("units").alias("units_bit_or"),
    )
)

result = (
    stats
    .select(
        "quarter_start",
        "distinct_ids",
        "num_rows",
        "avg_units",
        "cnt",
        "units_bit_or",
    )
    .orderBy(F.col("distinct_ids").desc_nulls_first())
)
