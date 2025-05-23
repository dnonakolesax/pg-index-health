/*
 * Copyright (c) 2019-2025. Ivan Vakhrushev and others.
 * https://github.com/mfvanek/pg-index-health-sql
 *
 * Licensed under the Apache License 2.0
 */

-- Finds tables that don't have a description. See also https://www.postgresql.org/docs/current/sql-comment.html
select
    pc.oid::regclass::text as table_name
from
    pg_catalog.pg_class pc
    inner join pg_catalog.pg_namespace nsp on nsp.oid = pc.relnamespace
where
    pc.relkind in ('r', 'p') and
    not pc.relispartition and
    (obj_description(pc.oid) is null or length(trim(obj_description(pc.oid))) = 0) and
    nsp.nspname = $1
order by table_name;
