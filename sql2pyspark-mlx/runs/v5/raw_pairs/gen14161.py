from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.orderBy(F.col("revenue").desc(), "id")
w3 = Window.partitionBy("store_id").orderBy("sale_date_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city").orderBy(F.col("last_known_units_filled").desc(), "id")

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-06-11")))
    .withColumn("units_filled", F.coalesce("units", F.lit(0)))
    .withColumn("sale_date_month", F.month("sale_date"))
)

with_windows = (
    cleaned
    .filter(F.month("sale_date").isin([2, 6, 7]))
    .select(
        "id",
        "store_id",
        "revenue",
        "units_filled",
        "sale_date_month",
        F.round((F.col("units") / F.sum("units").over(w)), 4).alias("units_share"),
        F.row_number().over(w2).alias("row_seq"),
        F.first("units_filled", ignorenulls=True).over(w3).alias("first_known_units_filled"),
        F.last("units_filled", ignorenulls=True).over(w3).alias("last_known_units_filled"),
        F.first("units", ignorenulls=True).over(w4).alias("first_known_units"),
    )
)

cleaned_store_sales = (
    with_windows
    .filter(F.col("first_known_units_filled").between(76, 77))
    .select(
        "*",
        (F.col("units_share") * F.col("units_filled")).alias("units_share_times_units_filled"),
        (F.col("row_seq") + F.col("sale_date_month")).alias("row_seq_plus_sale_date_month"),
    )
)

with_dims = (
    cleaned_store_sales
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select("e.*", F.col("s.city"), F.col("s.opened_date"), F.col("s.sqft"))
)

ranked = (
    with_dims
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .alias("e")
    .join(df_stores.alias("dim"), F.col("e.store_id") == F.col("dim.id"), "left")
    .rollup(
        F.col("dim.region").alias("region"),
        F.col("e.city").alias("store_sale_city"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("e.first_known_units")).alias("avg_first_known_units"),
        F.avg(F.col("e.first_known_units_filled")).alias("avg_first_known_units_filled"),
        F.grouping_id().alias("grouping_level"),
    )
)
