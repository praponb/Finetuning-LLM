from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("ship_country").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("ship_country").orderBy(F.col("created_at").desc(), "id")

flattened_attrs = (
    df_carts
    .filter(F.to_date("created_at") < "2025-08-10")
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "ship_to",
        "attributes",
        F.col("ship_to.country").alias("ship_country"),
        F.array_contains("coupon_codes", "SPRING10").alias("used_spring10"),
    )
)

result = (
    flattened_attrs
    .withColumn("first_known_ship_country", F.first("ship_country", ignorenulls=True).over(w_recent))
    .withColumn("last_known_ship_country", F.last("ship_country", ignorenulls=True).over(w_recent))
    .withColumn("row_seq", F.row_number().over(w2))
)
