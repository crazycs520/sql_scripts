drop table if exists t1;
CREATE TABLE t1 (c1 int);
INSERT INTO t1 SET c1 = 1;
ALTER TABLE t1 ADD COLUMN cc1 CHAR(36) NULL DEFAULT '';
ALTER TABLE t1 ADD INDEX idx1 (cc1);
admin check table t1;