create table
    scales(
        id          integer primary key generated always as identity,
        created_at  timestamptz,
        device_id   int8,
        user_id     uuid,
        registered  bool,
        name        text
    );

/*
insert into scales
 (device_id, user_id, registered, name)
values
 (39748960246732, 'e32f5583-c101-4bac-97eb-b77fe01109f1', true, 'kitchen'),
 (79741262426312, 'e32f5583-c101-4bac-97eb-b77fe01109f1', true, 'leftovers'),
 (123, '24360e50-e668-44cc-8cd9-7a6e9a10f4a8', false, 'bones');
 */