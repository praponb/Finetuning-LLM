from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("supplier_id", "brand").orderBy("launch_date", "id")
w2 = Window.partitionBy("category", "supplier_id").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-08-22").cast("date"))
    .withColumn("launch_date_month_end", F.last_day("launch_date"))
)

result = (
    filtered
    .select(
        "*",
        F.lag("cost").over(w_running).alias("prev_cost"),
        F.lead("price").over(w_running).alias("next_price"),
        F.row_number().over(w_running).alias("row_seq"),
        F.last("cost", ignorenulls=True).over(w2).alias("last_known_cost"),
    )
)
