from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date_month_end", "id")
w2 = Window.orderBy("revenue", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-03-10").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "revenue",
        F.add_months("sale_date", 12).alias("sale_date_plus_months"),
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "store_id",
        "sale_date_plus_months",
        "sale_date_month_end",
        F.lead("revenue").over(w).alias("next_revenue"),
        F.row_number().over(w).alias("row_seq"),
        F.avg("revenue").over(w2).alias("revenue_avg_6"),
    )
)
