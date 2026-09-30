from pyspark.sql import functions as F

with_dims = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("d"), F.col("a.supplier_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "a.id",
        "a.brand",
        "a.price",
        "a.launch_date",
        "a.supplier_id",
        F.col("d.country"),
        F.col("d.city"),
    )
)

result = with_dims.select("id", "price", "launch_date", "country", "city")
