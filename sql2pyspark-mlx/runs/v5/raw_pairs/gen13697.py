from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id", "category")
    .pivot("brand", ["Globex", "Umbrella", "Initech"])
    .agg(F.max("price"))
    .select(
        "supplier_id",
        "category",
        F.col("Globex").alias("globex"),
        F.col("Umbrella").alias("umbrella"),
        F.col("Initech").alias("initech"),
    )
)
