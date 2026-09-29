from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("least_units_revenue").desc_nulls_first(), "id")
w2 = Window.partitionBy("product_id", "store_id")
w3 = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-09-02").cast("date"))
    .withColumn("least_units_revenue", F.least("units", "revenue"))
)

valid_store_sales = (
    valid
    .filter(F.col("revenue") > 1800)
    .withColumn("revenue_per_unit", (F.col("units") + F.col("revenue")))
)

latest = (
    valid_store_sales
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

scored = (
    latest
    .filter(F.col("store_id") == 6)
    .select(
        "*",
        F.round((F.col("units") / F.sum("units").over(w2)), 4).alias("units_share"),
        F.sum("revenue_per_unit").over(w3).alias("cumulative_revenue_per_unit"),
    )
)

with_dims = (
    scored
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.units",
        "s.revenue",
        "s.least_units_revenue",
        "s.units_share",
        "s.cumulative_revenue_per_unit",
        F.col("b.brand"),
        F.col("b.launch_date"),
    )
)

result = (
    with_dims
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "least_units_revenue",
        "units_share",
        "cumulative_revenue_per_unit",
        "brand",
        "launch_date",
        F.when(F.col("brand").rlike("^(Globex|Acme)$"), "CORE").when(F.col("brand").rlike("^(Initech|Umbrella)$"), "GROUP_A").otherwise("PRIORITY").alias("brand_zone"),
    )
    .orderBy("product_id")
)
