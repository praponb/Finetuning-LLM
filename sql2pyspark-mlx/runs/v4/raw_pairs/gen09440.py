from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("product_id", "store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("revenue", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("product_id").orderBy("revenue", "id")

enriched = (
    df_store_sales
    .select(
        "*",
        F.count("revenue").over(w).alias("store_id_count_revenue"),
        F.sum("revenue").over(w2).alias("running_revenue"),
        F.avg("revenue").over(w3).alias("revenue_avg_2_prior"),
        F.lead("revenue", 2).over(w4).alias("next_revenue"),
    )
)

result = (
    enriched
    .select(
        "id",
        "sale_date",
        "revenue",
        "store_id_count_revenue",
        "running_revenue",
        "revenue_avg_2_prior",
        "next_revenue",
    )
    .orderBy("sale_date")
)
