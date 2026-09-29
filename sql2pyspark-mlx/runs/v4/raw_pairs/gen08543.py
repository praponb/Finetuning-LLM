from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "created_at",
        "items",
        "ship_to",
        F.forall("items", lambda item: item["qty"] <= 5).alias("all_small_qty"),
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
    )
)
