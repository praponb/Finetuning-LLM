from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("store_id", "product_id")
w3 = Window.partitionBy("product_id").orderBy(F.col("opened_date").desc(), "id")
w4 = Window.partitionBy("store_id").orderBy(F.col("tokyo_count").desc())

with_dims = (
    df_store_sales
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.sale_date",
        F.col("s.sqft"),
        F.col("s.opened_date"),
    )
)

with_windows = (
    with_dims
    .select(
        "*",
        F.min("sqft").over(w).alias("last7_min_sqft"),
        F.sum("sqft").over(w).alias("last7_sum_sqft"),
        F.count("sqft").over(w).alias("last7_count_sqft"),
        F.max("sqft").over(w2).alias("store_id_max_sqft"),
    )
)

with_dims_store_sales = (
    with_windows
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.sale_date",
        "a.opened_date",
        "a.last7_sum_sqft",
        "a.store_id_max_sqft",
        F.col("p.city"),
        F.col("p.store_name"),
        F.col("p.opened_date").alias("store_opened_date"),
    )
)

latest = (
    with_dims_store_sales
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .groupBy("store_id", "product_id", "city")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
    )
    .withColumn("tokyo_count_rank", F.rank().over(w4))
)
