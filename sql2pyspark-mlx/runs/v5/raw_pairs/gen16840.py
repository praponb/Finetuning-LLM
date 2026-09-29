from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id")
w_ordered_2 = Window.partitionBy("store_id")
w4 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.orderBy("store_id_count_revenue", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.partitionBy("store_id").orderBy("running_units_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("last3_min_running_units", "id")

scored = (
    df_store_sales
    .filter(F.col("revenue").isNotNull())
    .select(
        "id",
        "store_id",
        "sale_date",
        F.sum("units").over(w_ordered).alias("running_units"),
        F.row_number().over(w2).alias("row_seq"),
        F.count("revenue").over(w_ordered_2).alias("store_id_count_revenue"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "store_id",
        "sale_date",
        "running_units",
        F.min("running_units").over(w4).alias("last3_min_running_units"),
        F.max("running_units").over(w4).alias("last3_max_running_units"),
        F.avg("running_units").over(w4).alias("last3_avg_running_units"),
        F.avg("running_units").over(w5).alias("running_units_avg_5"),
    )
)

enriched_store_sales = (
    enriched
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.region"), F.col("s.opened_date"))
)

joined = (
    enriched_store_sales
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.store_id",
        "s.sale_date",
        "s.last3_min_running_units",
        "s.last3_max_running_units",
        "s.last3_avg_running_units",
        "s.running_units_avg_5",
        "s.opened_date",
        F.col("p.sqft"),
    )
)

enriched_store_sales_2 = (
    joined
    .select(
        "id",
        "store_id",
        "sale_date",
        "last3_min_running_units",
        "running_units_avg_5",
        "opened_date",
        "sqft",
        F.first("last3_min_running_units", ignorenulls=True).over(w6).alias("first_known_last3_min_running_units"),
        F.round((F.col("last3_max_running_units") / F.sum("last3_max_running_units").over(w_ordered_2)), 4).alias("last3_max_running_units_share"),
        F.lead("running_units_avg_5", 2).over(w7).alias("next_running_units_avg_5"),
    )
)

result = (
    enriched_store_sales_2
    .select(
        "id",
        "sale_date",
        "last3_min_running_units",
        "running_units_avg_5",
        "sqft",
        "first_known_last3_min_running_units",
        "last3_max_running_units_share",
    )
)
