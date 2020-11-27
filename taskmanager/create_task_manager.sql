create table task_manager
(
    -- Primary Key
    id           serial not null,

    -- Task Reference Id
    reference_id varchar(20),

    -- Task Metadata
    task_group   varchar(20),
    task_type    varchar(20),
    recurring    boolean,
    status       varchar(20),
    timeout      integer,
    message      varchar(512),
    properties   bytea,

    -- Record Timestamps
    created_at   timestamptz default now(),
    updated_at   timestamptz default now()
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

create trigger set_task_manager_updated_at_timestamp
    before update
    on task_manager
    for each row
execute procedure get_updated_at_timestamp();
