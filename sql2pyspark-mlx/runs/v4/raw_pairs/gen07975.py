from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["New York", "London"]))
    .select(
        "id",
        "country",
        "rating",
        F.coalesce("supplier_name", F.lit("unassigned")).alias("supplier_name_filled"),
    )
    .orderBy(F.col("rating").desc(), "id")
    .limit(10)
)
