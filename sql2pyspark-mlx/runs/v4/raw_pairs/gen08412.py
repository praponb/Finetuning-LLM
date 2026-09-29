from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("revenue").desc(), "id")
w_running = Window.partitionBy("product_id", "store_id").orderBy(F.col("units").desc_nulls_last())

scored = (
    df_store_sales
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.percent_rank().over(w_running).alias("units_percent_rank"),
    )
)

result = (
    scored
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        "row_seq",
        "units_percent_rank",
    )
    .orderBy(F.col("store_id").desc_nulls_first())
)
