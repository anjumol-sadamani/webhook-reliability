-- Polyfill for uuidv7() function for PostgreSQL < 18
-- This creates a UUIDv7 (time-ordered UUID) compatible function

CREATE OR REPLACE FUNCTION uuidv7() RETURNS uuid AS $$
DECLARE
    unix_ts_ms bytea;
    uuid_bytes bytea;
BEGIN
    -- Get current timestamp in milliseconds
    unix_ts_ms := substring(int8send(floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint) FROM 3);

    -- Build the UUID bytes:
    -- Bytes 0-5: timestamp (48 bits)
    -- Bytes 6-7: version (4 bits) + random (12 bits)
    -- Bytes 8-15: variant (2 bits) + random (62 bits)
    uuid_bytes := unix_ts_ms || gen_random_bytes(10);

    -- Set version to 7 (0111) in byte 6
    uuid_bytes := set_byte(uuid_bytes, 6, (get_byte(uuid_bytes, 6) & 15) | 112);

    -- Set variant to 2 (10xx) in byte 8
    uuid_bytes := set_byte(uuid_bytes, 8, (get_byte(uuid_bytes, 8) & 63) | 128);

    RETURN encode(uuid_bytes, 'hex')::uuid;
END
$$ LANGUAGE plpgsql VOLATILE;