from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("units").desc(), "id")
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("product_id").orderBy("units", "id")

valid = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-04-13").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        F.date_format("sale_date", "MMM yyyy").alias("sale_date_label"),
        F.coalesce("revenue", F.lit(0)).alias("revenue_filled"),
    )
)

result = (
    valid
    .select(
        "id",
        "store_id",
        "sale_date_label",
        "revenue_filled",
        F.row_number().over(w).alias("row_seq"),
        F.avg("units").over(w2).alias("store_id_avg_units"),
        F.lag("units", 2).over(w3).alias("prev_units"),
    )
)
