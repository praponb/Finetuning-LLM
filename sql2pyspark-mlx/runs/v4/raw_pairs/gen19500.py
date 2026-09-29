from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("greatest_units_revenue").desc())
w_hist = Window.partitionBy("store_id")
w_ordered = Window.orderBy(F.col("greatest_units_revenue").asc_nulls_last())

matched_keys = (
    df_store_sales
    .select("product_id", "store_id")
    .subtract(
        df_store_sales
        .filter(F.col("product_id").isin([5, 6, 10]))
        .select("product_id", "store_id")
    )
)

verified = (
    df_store_sales
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.store_name"))
)

base = (
    joined
    .filter((F.col("sale_date") > F.to_date(F.lit("2024-04-05"))) & F.col("store_id").isin([3, 4, 5]))
    .select(
        "id",
        "store_id",
        "sale_date",
        "store_name",
        F.greatest("units", "revenue").alias("greatest_units_revenue"),
        F.upper(F.trim("store_name")).alias("store_name_normalized"),
    )
)

metrics = (
    base
    .withColumn("greatest_units_revenue_percent_rank", F.percent_rank().over(w))
    .withColumn("store_id_avg_greatest_units_revenue", F.avg("greatest_units_revenue").over(w_hist))
    .withColumn("greatest_units_revenue_percent_rank_2", F.percent_rank().over(w_ordered))
    .withColumn("store_id_max_greatest_units_revenue", F.max("greatest_units_revenue").over(w_hist))
    .withColumn("store_id_sum_greatest_units_revenue", F.sum("greatest_units_revenue").over(w_hist))
)

with_dims = (
    metrics
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.city"))
)

result = (
    with_dims
    .groupBy("store_id")
    .pivot("city", ["Berlin", "Bangkok"])
    .agg(F.sum("greatest_units_revenue_percent_rank").alias("total"))
)
