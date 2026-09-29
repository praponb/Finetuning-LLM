from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("region").orderBy("sqft_filled", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("city", "region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-08-13").cast("date")) & F.col("store_name").like("Ri%"))
    .select("*", F.coalesce("sqft", F.lit(0)).alias("sqft_filled"))
)

result = (
    valid
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        F.avg("sqft").over(w).alias("sqft_avg_6"),
        F.avg("sqft_filled").over(w2).alias("sqft_filled_avg_5"),
        F.sum("sqft_filled").over(w3).alias("cumulative_sqft_filled"),
    )
)
