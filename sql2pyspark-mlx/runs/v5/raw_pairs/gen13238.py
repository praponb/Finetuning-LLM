from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.col("created_at") < "2024-10-01")
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        "attributes",
        F.transform("items", lambda li: li["product_id"]).alias("product_ids"),
        F.size(F.filter("items", lambda li: li["qty"] >= 3)).alias("bulk_lines"),
        F.coalesce(F.col("ship_to.country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
    )
)
