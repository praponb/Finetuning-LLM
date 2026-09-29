from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("units").desc_nulls_first())
w3 = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy("revenue_per_unit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("revenue_per_unit", "id")

valid = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-05-09").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit"),
        F.col("units").cast("double").alias("units_num"),
    )
)

scored = (
    valid
    .select(
        "id",
        "units",
        "revenue_per_unit",
        "units_num",
        F.lag("revenue_per_unit", 2).over(w).alias("prev_revenue_per_unit"),
        F.dense_rank().over(w2).alias("units_dense_rank"),
        F.last("units_num", ignorenulls=True).over(w3).alias("last_known_units_num"),
        F.last("units_num", ignorenulls=True).over(w4).alias("last_known_units_num_2"),
        F.lag("revenue", 1).over(w5).alias("prev_revenue"),
    )
)

base = (
    scored
    .filter(F.col("prev_revenue_per_unit") > 36)
    .withColumn("units_scaled", F.round((F.col("units") * 0.9), 2))
)

cleaned = (
    base
    .filter(F.col("revenue_per_unit").isNull())
    .select(
        "id",
        "units",
        "revenue_per_unit",
        "units_num",
        "prev_revenue_per_unit",
        "units_dense_rank",
        "last_known_units_num",
        "last_known_units_num_2",
        "prev_revenue",
        "units_scaled",
        F.col("last_known_units_num").cast("int").alias("last_known_units_num_int"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("last_known_units_num_int") >= 90, "PREMIUM").otherwise("STANDARD").alias("last_known_units_num_int_tier"),
        F.when(F.col("revenue_per_unit") >= 74, "LARGE").otherwise("SMALL").alias("revenue_per_unit_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.countDistinct("id").alias("distinct_ids_3"),
        F.count("*").alias("cnt"),
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids_4"),
    )
    .orderBy(F.col("distinct_ids_2").desc())
)
