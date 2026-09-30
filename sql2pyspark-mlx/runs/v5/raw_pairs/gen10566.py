from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("store_id").orderBy("greatest_revenue_units", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("greatest_revenue_units").desc())

base = (
    df_store_sales
    .filter(F.col("product_id").isin([5, 10]))
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        F.greatest("revenue", "units").alias("greatest_revenue_units"),
    )
)

result = (
    base
    .select(
        "id",
        "greatest_revenue_units",
        F.lag("units").over(w_ordered).alias("prev_units"),
        F.lead("greatest_revenue_units").over(w_ordered).alias("next_greatest_revenue_units"),
        F.row_number().over(w_ordered).alias("event_seq"),
        F.cume_dist().over(w2).alias("greatest_revenue_units_cume_dist"),
    )
)
