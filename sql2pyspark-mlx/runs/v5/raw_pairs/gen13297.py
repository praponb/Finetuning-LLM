from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("store_id").orderBy("sqft", "id")
w2 = Window.partitionBy("store_region").orderBy("sqft", "id").rowsBetween(-6, Window.currentRow)
w_grp = Window.partitionBy("brand", "region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_store_sales
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.product_id",
        "e.sale_date",
        "e.units",
        F.col("d.brand"),
    )
)

joined = (
    with_dims
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select("e.*", F.col("s.region"), F.col("s.store_name"), F.col("s.sqft"))
)

enriched = (
    joined
    .select(
        "*",
        F.lead("sqft").over(w_ordered).alias("next_sqft"),
        F.lag("units").over(w_ordered).alias("prev_units"),
        F.row_number().over(w_ordered).alias("row_seq"),
    )
)

joined_store_sales = (
    enriched
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "inner")
    .select(
        "e.*",
        F.col("p.region").alias("store_region"),
        F.col("p.store_name").alias("store_store_name"),
        F.col("p.sqft").alias("store_sqft"),
    )
)

enriched_store_sales = (
    joined_store_sales
    .withColumn("last7_count_units", F.count("units").over(w2))
    .withColumn("last7_avg_units", F.avg("units").over(w2))
    .withColumn("last7_sum_units", F.sum("units").over(w2))
    .withColumn("last7_min_units", F.min("units").over(w2))
)

metrics = (
    enriched_store_sales
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "region",
        "sqft",
        "next_sqft",
        "prev_units",
        "row_seq",
        "store_region",
        "last7_sum_units",
        F.max("last7_sum_units").over(w_grp).alias("running_max_last7_sum_units"),
        F.min("last7_sum_units").over(w_grp).alias("running_min_last7_sum_units"),
        F.avg("last7_sum_units").over(w_grp).alias("running_avg_last7_sum_units"),
        F.sum("last7_sum_units").over(w_grp).alias("running_sum_last7_sum_units"),
    )
)

result = (
    metrics
    .groupBy("store_region", "store_id")
    .pivot("region", ["South", "Central"])
    .agg(F.max("units").alias("value_sum"))
    .select(
        "store_region",
        "store_id",
        F.col("South").alias("south"),
        F.col("Central").alias("central"),
    )
)
