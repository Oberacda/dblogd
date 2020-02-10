BEGIN;
create table IF NOT EXISTS sensor_name
(
    id   bigserial not null
        constraint pk_sensor_name
            primary key,
    name text      not null
);

create table IF NOT EXISTS records
(
    id        bigserial                not null
        constraint pk_records
            primary key,
    timestamp timestamp with time zone not null,
    sensor_id bigint                   not null
        constraint fk_records_sensor_name
            references sensor_name
            on update cascade
);

create table IF NOT EXISTS humidity
(
    record_id bigint           not null
        constraint pk_humidity
            primary key
        constraint fk_records_humidity
            references records
            on update cascade on delete cascade,
    humidity  double precision not null
);

create table IF NOT EXISTS illuminance
(
    record_id   bigint           not null
        constraint pk_illuminance
            primary key
        constraint fk_records_illuminance
            references records
            on update cascade on delete cascade,
    illuminance double precision not null
);

create table IF NOT EXISTS pressure
(
    record_id bigint           not null
        constraint pk_pressure
            primary key
        constraint fk_records_pressure
            references records
            on update cascade on delete cascade,
    pressure  double precision not null
);

create table IF NOT EXISTS temperature
(
    record_id   bigint           not null
        constraint pk_temperature
            primary key
        constraint fk_records_temperature
            references records
            on update cascade on delete cascade,
    celsius double precision not null
);

create table IF NOT EXISTS uv_index
(
    record_id bigint           not null
        constraint pk_uv_index
            primary key
        constraint fk_records_uv_index
            references records
            on update cascade on delete cascade,
    uv_index  double precision not null
);

create table IF NOT EXISTS uva
(
    record_id bigint           not null
        constraint pk_uva
            primary key
        constraint fk_records_uva
            references records
            on update cascade on delete cascade,
    uva       double precision not null
);

create table IF NOT EXISTS uvb
(
    record_id bigint           not null
        constraint pk_uvb
            primary key
        constraint fk_records_uvb
            references records
            on update cascade on delete cascade,
    uvb       double precision not null
);

create view IF NOT EXISTS records_environemental
            (id, timestamp, name, celsius, humidity, pressure, illuminance, uva, uvb, uv_index) as
SELECT rec.id,
       rec."timestamp",
       sen.name,
       tem.celsius,
       hum.humidity,
       pres.pressure,
       illu.illuminance,
       uva.uva,
       uvb.uvb,
       uv_index.uv_index
FROM records rec
         LEFT JOIN sensor_name sen ON rec.sensor_id = sen.id
         JOIN temperature tem ON rec.id = tem.record_id
         JOIN humidity hum ON rec.id = hum.record_id
         JOIN pressure pres ON pres.record_id = rec.id
         JOIN illuminance illu ON rec.id = illu.record_id
         JOIN uva uva ON rec.id = uva.record_id
         JOIN uvb uvb ON rec.id = uvb.record_id
         JOIN uv_index uv_index ON rec.id = uv_index.record_id;
COMMIT;