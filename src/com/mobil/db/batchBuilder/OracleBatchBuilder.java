package com.mobil.db.batchBuilder;

import com.mobil.db.batch.InsertBatch;
import com.mobil.db.object.Column;
import com.mobil.type.BATCH_TYPE;

import java.sql.Connection;
import java.util.Vector;


/**
 * Created by user on 2019-06-19.
 */
public class OracleBatchBuilder {

    public IBatch doBatchBuild(BATCH_TYPE bType, Connection con, String strTableName, Vector<Column> columnVec) throws Exception {

        IBatch iBatch = null;

        switch (bType) {
            case INSERT:
                iBatch = new InsertBatch(con, strTableName, columnVec);
                break;
            case UPDATE:
                break;
            case DELETE:
                break;
            case MERGE:
                break;
            default:
                throw new Exception("Undefined BATCH_TYPE!" + bType.toString());
        }

        return iBatch;
    }

}
