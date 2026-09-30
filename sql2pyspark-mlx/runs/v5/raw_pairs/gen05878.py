from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("weight_kg").desc(), "id")
w2 = Window.orderBy("created_at", "id")

flattened_attrs = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("attributes").getItem("weight_kg").try_cast("double").alias("weight_kg"),
        F.array_contains("coupon_codes", "SPRING10").alias("used_spring10"),
    )
)

result = (
    flattened_attrs
    .filter(F.to_date("created_at") < "2023-06-27")
    .withColumn("weight_kg_bucket", F.ntile(10).over(w))
    .withColumn("next_weight_kg", F.lead("weight_kg", 1).over(w2))
)
