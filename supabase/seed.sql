create table
    scales(
        id          integer primary key generated always as identity,
        created_at  timestamptz,
        device_id   int8,
        user_id     uuid,
        registered  bool,
        name        text
    );