package com.mobil.db.batchBuilder;

import java.sql.ResultSet;


public interface IBatch {

   int runBatch(ResultSet rset, int nMaxCount) throws Exception;

}
