from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("city").isin(["Tokyo", "Berlin"]) & F.col("city").isin(["Berlin", "London", "Singapore"]))
    .select(
        "rating",
        "city",
        "country",
        F.substring("country", 1, 3).alias("country_prefix"),
        F.regexp_replace("supplier_name", " ", "").alias("supplier_name_slug"),
    )
)
