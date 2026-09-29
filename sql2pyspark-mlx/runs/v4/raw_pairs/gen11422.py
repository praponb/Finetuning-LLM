from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("max_item_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("num_attributes", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.orderBy(F.col("max_item_price").desc(), "id")
w4 = Window.orderBy("num_attributes", "id")
w5 = Window.orderBy(F.col("max_item_price").desc_nulls_first())

flattened_attrs = (
    df_carts
    .select(
        "id",
        "ship_to",
        F.size("attributes").alias("num_attributes"),
        F.array_max(F.transform("items", lambda x: x["price"])).alias("max_item_price"),
        F.forall("items", lambda x: x["qty"] <= 5).alias("all_small_qty"),
        F.array_contains("coupon_codes", "SPRING10").alias("used_spring10"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("none")).alias("utm_source_or_default"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.first("max_item_price", ignorenulls=True).over(w).alias("first_known_max_item_price"),
        F.avg("max_item_price").over(w2).alias("max_item_price_avg_6"),
        F.row_number().over(w3).alias("event_seq"),
        F.lead("max_item_price", 1).over(w4).alias("next_max_item_price"),
        F.rank().over(w5).alias("max_item_price_rank"),
    )
)
