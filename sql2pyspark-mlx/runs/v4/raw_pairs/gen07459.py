from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue_per_unit", "id")
w2 = Window.orderBy("revenue_per_unit", "id")
w3 = Window.partitionBy("store_id", "product_id").orderBy(F.col("revenue_per_unit").desc_nulls_last())

filtered = (
    df_store_sales
    .filter(F.col("store_id") == 5)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        (F.col("revenue") + F.col("units")).alias("revenue_per_unit"),
    )
)

result = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.lead("revenue_per_unit", 1).over(w2).alias("next_revenue_per_unit"),
        F.rank().over(w3).alias("revenue_per_unit_rank"),
    )
)
