CREATE TABLE segments_metadata (
    id           serial       primary key not null,
    hash         varchar(100) not null unique,
    file_name    varchar(100) not null,
    file_offset  bigint       not null,
    reference    integer      not null
);

create index on segments_metadata using hash(hash);