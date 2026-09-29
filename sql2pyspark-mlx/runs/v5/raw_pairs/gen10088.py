from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "ship_to",
        "attributes",
        F.coalesce(F.col("attributes").getItem("gift"), F.lit("unknown")).alias("gift_or_default"),
        F.transform("items", lambda item: item["product_id"]).alias("product_ids"),
    )
)
