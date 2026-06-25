# Block 9: verify RI — counts of fact rows whose FK is missing from its dimension
f = spark.table("fact_sales")
checks = {
 "date":     ("date_key",     "dim_date"),     "store":   ("store_key",     "dim_store"),
 "product":  ("product_key",  "dim_product"),  "category":("category_key",  "dim_category"),
 "supplier": ("supplier_key", "dim_supplier"), "promo":   ("promotion_key", "dim_promotion"),
 "channel":  ("channel_key",  "dim_channel"),
}
for label,(col,dim) in checks.items():
    orphans = f.join(spark.table(dim), col, "left_anti").count()
    print(f"{label:9s} orphan FKs: {orphans}")