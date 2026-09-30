from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("cost", "id")
w2 = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_products
    .filter(F.col("cost") > 490)
    .select(
        "id",
        "product_name",
        "price",
        "launch_date",
        "supplier_id",
        F.ntile(5).over(w).alias("cost_bucket"),
        F.last("brand", ignorenulls=True).over(w2).alias("last_known_brand"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "product_name",
        "launch_date",
        "supplier_id",
        "cost_bucket",
        F.when(F.col("product_name").rlike("^(Noise-Cancel Headphones|Yoga Mat)$"), "GROUP_A").otherwise("EXTENDED").alias("product_name_zone"),
    )
)
