-- ---------------------------------------------------------------------------
-- Creates one database per service.
--
-- The Postgres container runs every script in this directory exactly once, on
-- its first start (when the data volume is empty). The services themselves
-- create their own tables with Hibernate (`ddl-auto: update`), so only the
-- databases have to exist up front.
--
-- Each service gets its own database so that one service can be dropped and
-- recreated without touching the data of the others.
-- ---------------------------------------------------------------------------

CREATE DATABASE user_db;
CREATE DATABASE account_db;
CREATE DATABASE fund_transfer_db;
CREATE DATABASE cbs_db;
CREATE DATABASE notification_db;
