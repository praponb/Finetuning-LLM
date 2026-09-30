from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").between("2024-07-02", "2024-11-01"))
    .select(
        "id",
        "launch_date",
        "supplier_id",
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
    )
)
