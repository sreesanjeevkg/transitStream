create database transitStream;

create table trip_updates
(
    trip_id    varchar(25) not null,
    start_time time        not null,
    start_date date        not null,
    stop_seq   integer     not null,
    stop_id    varchar(25) not null,
    stop_time  timestamp,
    delay      integer     not null,
    status     varchar(25) not null,
    updated_at timestamp default CURRENT_TIMESTAMP,
    primary key (trip_id, start_time, start_date, stop_seq)
);

create table vehicle_updates

