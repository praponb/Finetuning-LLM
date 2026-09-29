from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("units_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("units").desc())

filtered = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-10-18").cast("date"))
    .select("id", "units", "revenue", F.col("units").cast("double").alias("units_num"))
)

result = (
    filtered
    .select(
        "*",
        F.last("units_num", ignorenulls=True).over(w).alias("last_known_units_num"),
        F.dense_rank().over(w2).alias("units_dense_rank"),
    )
)
