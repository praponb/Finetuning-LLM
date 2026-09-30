from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_carts
    .select(
        "id",
        "created_at",
        "items",
        "attributes",
        F.coalesce(F.col("attributes").getItem("utm_source"), F.lit("unknown")).alias("utm_source_or_default"),
        F.col("ship_to.zip").alias("ship_zip"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "created_at",
        "utm_source_or_default",
        F.first("utm_source_or_default", ignorenulls=True).over(w).alias("first_known_utm_source_or_default"),
        F.first("utm_source_or_default", ignorenulls=True).over(w).alias("first_known_utm_source_or_default_2"),
    )
)
