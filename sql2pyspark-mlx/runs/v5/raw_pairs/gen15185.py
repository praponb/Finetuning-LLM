from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("region", "city").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("store_sale_revenue", "id")

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("opened_date") >= F.to_date(F.lit("2023-03-13"))))
    .select("*", F.last_day("opened_date").alias("opened_date_month_end"))
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

with_last = (
    with_latest
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue"))
)

with_last_stores = (
    with_last
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.revenue").alias("store_sale_revenue"),
        F.col("c.units").alias("store_sale_units"),
    )
)

scored = (
    with_last_stores
    .select(
        "id",
        "store_name",
        "city",
        "sqft",
        "opened_date_month_end",
        "quantity",
        "store_sale_revenue",
        "store_sale_units",
        F.avg("store_sale_units").over(w_ordered).alias("running_avg_store_sale_units"),
        F.max("store_sale_units").over(w_ordered).alias("running_max_store_sale_units"),
        F.count("store_sale_units").over(w_ordered).alias("running_count_store_sale_units"),
        F.sum("store_sale_units").over(w_ordered).alias("running_sum_store_sale_units"),
        F.row_number().over(w_part).alias("row_seq"),
    )
)

result = (
    scored
    .groupBy(
        F.date_trunc("QUARTER", "opened_date_month_end").alias("quarter_start"),
        "city",
    )
    .agg(
        F.round(F.avg("running_sum_store_sale_units"), 2).alias("avg_running_sum_store_sale_units"),
        F.avg("running_sum_store_sale_units").alias("avg_running_sum_store_sale_units_2"),
        F.avg("sqft").alias("avg_sqft"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count_if(F.col("city") == "New York").alias("new_york_events"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
    )
)
