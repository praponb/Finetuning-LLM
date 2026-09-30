from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "created_at",
        "items",
        "attributes",
        F.array_max(F.transform("items", lambda li: li["price"])).alias("max_item_price"),
        F.col("attributes")["gift"].alias("gift"),
    )
)
