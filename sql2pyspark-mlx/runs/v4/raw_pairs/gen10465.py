from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("product_id", "store_id").orderBy("revenue_per_unit", "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("units_whole").desc_nulls_first())

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-06-01").cast("date")) & F.col("revenue").isNull())
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit"),
        F.floor("units").alias("units_whole"),
    )
)

filtered = (
    cleaned
    .filter(F.col("units_whole").between(30, 74))
    .withColumn("revenue_per_unit_adjusted", F.round((F.col("revenue_per_unit") * 0.9), 2))
)

with_windows = (
    filtered
    .withColumn("units_share", F.round((F.col("units") / F.sum("units").over(w)), 4))
    .withColumn("next_units_whole", F.lead("units_whole", 1).over(w2))
    .withColumn("units_whole_percent_rank", F.percent_rank().over(w3))
)

with_dims = (
    with_windows
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.region"), F.col("p.sqft"))
)

result = (
    with_dims
    .select(
        "id",
        "product_id",
        "revenue",
        "revenue_per_unit",
        "revenue_per_unit_adjusted",
    )
    .unpivot(
        ["id", "product_id"],
        ["revenue", "revenue_per_unit", "revenue_per_unit_adjusted"],
        "measure",
        "val",
    )
)
