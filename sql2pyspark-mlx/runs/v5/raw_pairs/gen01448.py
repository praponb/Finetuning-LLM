from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("customer_id").orderBy(F.col("created_at").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy("created_at", "id")

flattened_attrs = (
    df_carts
    .filter(F.col("created_at") < "2024-09-05")
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "ship_to",
        "attributes",
        F.transform("items", lambda i: i["product_id"]).alias("product_ids"),
        F.col("attributes").getItem("utm_source").alias("utm_source"),
    )
)

result = (
    flattened_attrs
    .filter(F.array_contains("coupon_codes", "WELCOME5"))
    .select(
        "*",
        F.first("utm_source", ignorenulls=True).over(w).alias("first_known_utm_source"),
        F.row_number().over(w_grp).alias("seq_no"),
        F.row_number().over(w3).alias("row_seq"),
    )
)
