drop table if exists msg_queue;
create table msg_queue(
    msg_id integer not null,
    queue_type varchar(10) not null, -- type1, type2 ...
    status varchar(1) not null, -- N (new), P (process), D (done)
    order_id integer not null, -- records with same order_id and queue_type have to be handled in correct order, 1,2,3,4...
    time_added timestamp with time zone not null,
    time_processed timestamp with time zone
);
create unique index msg_queue_uniq_msg_id on msg_queue(msg_id);
create index msg_queue_time_added on msg_queue(time_added);
