from sqlalchemy import DDL

# Define the function creation DDL
calculate_systime_range_ddl = DDL(
    """
CREATE OR REPLACE FUNCTION common.calculate_systime_range(
    effective_time TIMESTAMP,
    current_time_utc TIMESTAMP
)
RETURNS TSRANGE AS $$
BEGIN
    IF abs(extract(epoch from (current_time_utc - effective_time))) < 0.01 THEN
        RETURN tstzrange(effective_time, 'infinity', '[)');
    ELSE
        RETURN tstzrange(current_time_utc, 'infinity', '[)');
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
"""
)
