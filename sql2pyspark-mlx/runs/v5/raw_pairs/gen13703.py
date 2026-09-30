from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w_hist = Window.partitionBy("city").orderBy("rating_int", "id")

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.length("supplier_name") >= 8))
    .select(
        "id",
        "city",
        "rating",
        F.when(F.col("city") == "Chiang Mai", 1).otherwise(0).alias("is_chiang_mai"),
        F.coalesce("city", F.lit("OTHER")).alias("resolved_city"),
    )
)

cleaned_suppliers = (
    cleaned
    .filter(F.col("is_chiang_mai") == 40)
    .select(
        "id",
        "city",
        "rating",
        "is_chiang_mai",
        "resolved_city",
        F.when(F.col("is_chiang_mai") >= 71, "High").when(F.col("is_chiang_mai") >= 22, "Medium").otherwise("Low").alias("is_chiang_mai_band"),
        F.trim("city").alias("clean_city"),
        F.coalesce("city", F.lit("OTHER")).alias("resolved_city_2"),
    )
)

cleaned_suppliers_2 = (
    cleaned_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("rating_int", F.col("rating").cast("int"))
    .withColumn("city_filled", F.coalesce("city", F.lit("DIRECT")))
)

scored = (
    cleaned_suppliers_2
    .select(
        "*",
        F.max("is_chiang_mai").over(w).alias("city_max_is_chiang_mai"),
        F.row_number().over(w_hist).alias("seq_no"),
    )
)

result = (
    scored
    .cube(
        F.when(F.col("seq_no") >= 8, "HIGH").otherwise("LOW").alias("seq_no_tier"),
        "city",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_rows"),
    )
    .filter(F.col("new_york_rows") >= 1)
    .orderBy("new_york_rows")
)
