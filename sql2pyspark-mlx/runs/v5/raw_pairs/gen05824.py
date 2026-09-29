from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("weight_kg").desc_nulls_last())

flattened_attrs = (
    df_carts
    .select(
        "id",
        "items",
        "attributes",
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
        F.aggregate("items", F.lit(0.0), lambda acc, item: acc + item["qty"] * item["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.size("attributes").alias("num_attributes"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("num_attributes").between(51, 52))
    .select(
        "id",
        "bulk_lines",
        F.last("bulk_lines", ignorenulls=True).over(w).alias("last_known_bulk_lines"),
        F.percent_rank().over(w2).alias("weight_kg_percent_rank"),
    )
)
