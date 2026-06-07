-- Idempotent dimension upsert mirroring merge_dim_user.sql's pattern (the spec gives
-- DIM_USER's MERGE verbatim and asks for DIM_PRODUCT's "mirroring the pattern" —
-- this file is the inferred mirror). DIM_PRODUCT has no dw_updated_at column (see DDL),
-- so MATCHED rows simply refresh the descriptive fields.
MERGE INTO ANALYTICS.DIM_PRODUCT AS target
USING STAGING.PRODUCTS_CLEAN AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN UPDATE SET
    product_name = source.product_name,
    category     = source.category,
    price        = source.price
WHEN NOT MATCHED THEN INSERT
    (product_id, product_name, category, price)
VALUES
    (source.product_id, source.product_name, source.category, source.price);
