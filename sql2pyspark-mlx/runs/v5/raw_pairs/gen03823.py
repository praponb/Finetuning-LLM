from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("product_id", "store_id").orderBy("sale_date_day", "id")
w2 = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_store_sales
    .filter(F.col("units") >= 22)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.dayofmonth("sale_date").alias("sale_date_day"),
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)

result = (
    valid
    .filter(F.col("product_id") == 3)
    .select(
        "id",
        "store_id",
        "revenue",
        F.lead("sale_date_day").over(w_hist).alias("next_sale_date_day"),
        F.lag("sale_date_day").over(w_hist).alias("prev_sale_date_day"),
        F.lead("revenue").over(w_hist).alias("next_revenue"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.first("units", ignorenulls=True).over(w2).alias("first_known_units"),
    )
)
