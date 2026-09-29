from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("units").desc_nulls_first())
w2 = Window.partitionBy("region").orderBy("sale_date", "id")
w3 = Window.partitionBy("store_id")
w_grp = Window.partitionBy("product_id").orderBy("next_units", "id")
w5 = Window.partitionBy("product_id")

joined = (
    df_store_sales
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.product_id",
        "e.sale_date",
        "e.units",
        "e.revenue",
        F.col("s.region"),
        F.col("s.sqft"),
        F.col("s.store_name"),
    )
)

with_windows = (
    joined
    .withColumn("units_percent_rank", F.percent_rank().over(w))
    .withColumn("next_units", F.lead("units", 2).over(w2))
    .withColumn("store_id_count_sqft", F.count("sqft").over(w3))
)

with_dims = (
    with_windows
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.category") == "Fashion")
    .select(
        "s.id",
        "s.product_id",
        "s.sale_date",
        "s.units",
        "s.revenue",
        "s.sqft",
        "s.next_units",
        F.col("p.launch_date"),
        F.col("p.product_name"),
    )
)

metrics = (
    with_dims
    .select(
        "id",
        "units",
        "revenue",
        "sqft",
        "next_units",
        "product_name",
        F.lead("revenue").over(w_grp).alias("next_revenue"),
        F.lead("next_units").over(w_grp).alias("next_next_units"),
        F.row_number().over(w_grp).alias("event_seq"),
        F.min("units").over(w5).alias("product_id_min_units"),
    )
)

summary = (
    metrics
    .groupBy(
        F.when(F.col("product_id_min_units") >= 40, "LARGE").otherwise("SMALL").alias("product_id_min_units_tier"),
        F.when(F.col("units") >= 38, "LARGE").otherwise("SMALL").alias("units_tier"),
    )
    .agg(
        F.stddev_samp("revenue").alias("revenue_stddev"),
        F.sum("product_id_min_units").alias("total_product_id_min_units"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
    .filter(F.col("revenue_stddev") > 0)
)

result = (
    summary
    .select("product_id_min_units_tier", "units_tier", "total_product_id_min_units")
    .orderBy(
        F.col("product_id_min_units_tier").desc_nulls_last(),
        F.col("total_product_id_min_units").desc(),
    )
)
