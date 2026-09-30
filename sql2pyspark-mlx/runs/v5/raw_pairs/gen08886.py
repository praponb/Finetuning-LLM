from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("bulk_lines", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("num_attributes", "id")

parsed = (
    df_carts
    .filter(F.array_contains("coupon_codes", "SPRING10"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "items",
        "ship_to",
        "attributes",
        F.size(F.filter("items", lambda x: x["qty"] >= 3)).alias("bulk_lines"),
        F.size("attributes").alias("num_attributes"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.size("items").alias("num_items"),
    )
)

result = (
    parsed
    .select(
        "id",
        "customer_id",
        "num_attributes",
        "num_items",
        F.last("num_attributes", ignorenulls=True).over(w).alias("last_known_num_attributes"),
        F.ntile(10).over(w2).alias("num_attributes_bucket"),
    )
)
