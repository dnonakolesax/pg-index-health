/*
 * Copyright (c) 2019-2025. Ivan Vakhrushev and others.
 * https://github.com/mfvanek/pg-index-health-sql
 *
 * Licensed under the Apache License 2.0
 */

-- Finds tables that don't have a primary key.
select
    pc.oid::regclass::text as table_name,
    pg_table_size(pc.oid) as table_size
from
    pg_catalog.pg_class pc
    inner join pg_catalog.pg_namespace nsp on nsp.oid = pc.relnamespace
where
    pc.relkind in ('r', 'p') and
    /* here we do not filter by pc.relispartition, because very often primary keys are created only for partitions */
    pc.oid not in (
        select c.conrelid as table_oid
        from pg_catalog.pg_constraint c
        where c.contype = 'p'
    ) and
    nsp.nspname = $1
order by table_name;
