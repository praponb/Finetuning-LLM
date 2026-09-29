from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("store_id")
w2 = Window.partitionBy("store_city").orderBy("opened_date", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("city").orderBy(F.col("units").asc_nulls_last())
w4 = Window.partitionBy("store_id", "store_city").orderBy("sale_date_month_end", "id")
w_hist = Window.partitionBy("store_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("store_id").orderBy(F.col("units_dense_rank").desc(), "id")
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.min("quantity").alias("min_inventory_quantity"))

filtered = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-06-20").cast("date"))
    .select(
        "id",
        "store_id",
        "units",
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)

joined = (
    filtered
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "left")
    .filter(F.col("d.opened_date") >= "2024-12-07")
    .select("s.*", F.col("d.city"))
)

joined_store_sales = (
    joined
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .select(
        "a.*",
        F.col("s.store_name"),
        F.col("s.sqft"),
        F.col("s.city").alias("store_city"),
    )
)

enriched = (
    joined_store_sales
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "left")
    .select(
        "a.*",
        F.col("p.opened_date"),
        F.col("p.store_name").alias("store_store_name"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "store_id",
        "units",
        "sqft",
        "store_store_name",
        F.count("sqft").over(w_grp).alias("store_id_count_sqft"),
        F.avg("units").over(w2).alias("units_avg_5"),
        F.dense_rank().over(w3).alias("units_dense_rank"),
        F.row_number().over(w4).alias("seq_no"),
        F.first("sqft", ignorenulls=True).over(w_hist).alias("first_known_sqft"),
    )
)

joined_store_sales_2 = (
    metrics
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.store_id",
        "a.store_store_name",
        "a.units_avg_5",
        "a.units_dense_rank",
        F.col("d.opened_date"),
    )
)

ranked = (
    joined_store_sales_2
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "store_id", "store_store_name", "units_avg_5", "units_dense_rank")
)

result = (
    ranked
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "store_store_name",
        "units_avg_5",
        "units_dense_rank",
        F.col("min_inventory_quantity"),
    )
    .orderBy(F.col("min_inventory_quantity").desc_nulls_first())
)
