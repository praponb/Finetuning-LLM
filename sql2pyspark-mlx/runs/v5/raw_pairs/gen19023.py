from pyspark.sql import functions as F

filtered = (
    df_suppliers
    .filter((~F.col("city").eqNullSafe(F.lit("Bangkok"))) & (F.col("rating") >= 4.1))
    .select("*", F.initcap(F.trim("supplier_name")).alias("supplier_name_title"))
)

result = (
    filtered
    .select("id")
    .subtract(
        df_suppliers
        .filter(F.col("city").isin(["Tokyo", "London"]))
        .select("id")
    )
)
