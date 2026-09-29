from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue", "id")
w2 = Window.partitionBy("product_id").orderBy("revenue_per_unit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-06-30")))
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        (F.col("units") - F.col("revenue")).alias("revenue_per_unit"),
        F.round("revenue", 1).alias("revenue_rounded"),
    )
)

result = (
    cleaned
    .filter(F.col("revenue").between(1100, 2000))
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("cumulative_units", F.sum("units").over(w2))
)
