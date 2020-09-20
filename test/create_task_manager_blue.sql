create table task_manager_blue
(
    -- Primary Key
    id           serial not null,

    -- Task Reference Id
    reference_id varchar(20),

    -- Task Metadata
    task_type    varchar(20),
    status       varchar(20),
    message      varchar(512),
    timeout      integer,
    properties   bytea,

    -- Record Timestamps
    created_at   timestamp with time zone default now(),
    updated_at   timestamp with time zone default now()
);

create or replace function get_updated_at_timestamp() returns trigger
    language plpgsql
as
$$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

create trigger set_task_manager_blue_updated_at_timestamp
    before update
    on task_manager_blue
    for each row
execute procedure get_updated_at_timestamp();
