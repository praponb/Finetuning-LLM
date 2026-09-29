from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("launch_date_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("supplier_id")
w3 = Window.orderBy("price", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc(), "id")
w5 = Window.partitionBy("supplier_id", "brand").orderBy(F.col("price").desc(), "id")

filtered = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.add_months("launch_date", 1).alias("launch_date_plus_months"),
    )
)

result = (
    filtered
    .filter(F.col("cost") < 720)
    .select(
        "id",
        "brand",
        "tags",
        "launch_date_plus_months",
        F.last("price", ignorenulls=True).over(w).alias("last_known_price"),
        F.max("cost").over(w2).alias("supplier_id_max_cost"),
        F.avg("price").over(w3).alias("price_avg_3_prior"),
        F.ntile(4).over(w4).alias("cost_bucket"),
        F.row_number().over(w5).alias("event_seq"),
    )
)
