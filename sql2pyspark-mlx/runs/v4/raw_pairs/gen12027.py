from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id")
w2 = Window.partitionBy("store_id").orderBy(F.col("revenue_filled_per_units").desc_nulls_first())

filtered = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2024-04-11").cast("date"))
    .select("*", F.coalesce("revenue", F.lit(0)).alias("revenue_filled"))
)

valid = (
    filtered
    .filter(F.col("sale_date") > F.lit("2023-05-04").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue_filled",
        F.round((F.col("revenue_filled") / F.col("units")), 2).alias("revenue_filled_per_units"),
        F.greatest("revenue", "units").alias("greatest_revenue_units"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.avg("units").over(w_part).alias("store_id_avg_units"),
        F.rank().over(w2).alias("revenue_filled_per_units_rank"),
    )
)

result = scored.select("id", "units")
