select /*+ TIDB_INLJ(t1,t2) */ count(*) from t1,t2 where t1.id!=t2.id and t1.name!=t2.name and t1.name<"name_abcd_2";

