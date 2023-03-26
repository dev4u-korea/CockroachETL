package com.mobil.db.batch;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.*;
import java.sql.ResultSet;

import com.mobil.db.batchBuilder.IBatch;
import com.mobil.db.object.Column;
import com.mobil.util.EasyLogger;
import com.mobil.util.CommonUtil;
import com.mobil.constant.DBINFO;


public class InsertBatch implements IBatch {

    private PreparedStatement _pstmt;
    private Connection _con;
    private Vector<Column> _columnVec;


    public InsertBatch(Connection con, String strTableName, Vector<Column> columnVec) throws Exception{
        _con = con;
        _con.setAutoCommit(false);

        StringBuilder _sbQry;
        _sbQry = new StringBuilder();
        _sbQry.append("INSERT INTO ");
        _sbQry.append(strTableName);
        _sbQry.append(" (");

        _columnVec = columnVec;

        StringBuilder sbValues = new StringBuilder();
        sbValues.append(" VALUES ( ");

        int nTotalColumnCount = _columnVec.size();

        for(Column col:_columnVec) {
            nTotalColumnCount--;

            if(nTotalColumnCount == 0) {
                _sbQry.append(col.getColumnName());
                sbValues.append("?");
            } else {
                _sbQry.append(col.getColumnName()).append(",");
                sbValues.append("?, ");
            }
        }

        _sbQry.append(")");
        sbValues.append(")");

        _sbQry.append(sbValues.toString());

        System.out.println("INSERT QRY = " + _sbQry.toString());

        _pstmt = con.prepareStatement(_sbQry.toString());
    }

    private boolean executeBatch(Vector<List<Column>> data) throws Exception {
        if(_pstmt == null) return false;

        _pstmt.clearBatch();

        for (List<Column> list:data) {

            int idx = 1;

            for(Column col: list) {

                switch (col.getDataType()){
                    case  STRING:
                        _pstmt.setString(idx, col.getDataValue());
                        break;
                    case INTEGER:
                        _pstmt.setInt(idx, Integer.parseInt(col.getDataValue()));
                        break;
                    case DOUBLE:
                        //_pstmt.setDouble(idx, Double.parseDouble(col.getDataValue()));
                        break;
                    case DATE:
                        /*if(col.getDataValue().equalsIgnoreCase("SYSDATE")) {
                            _pstmt.setTimestamp(idx, DateUtil.getTimestamp());
                        }
                        */
                        break;
                    default:
                        throw new Exception("Undefined DATA_TYPE = " + col.getDataType().toString());
                }

                idx++;
            }

            _pstmt.addBatch();

        }

        _pstmt.executeBatch();

        _con.commit();

        return true;
    }

    /*
        CockroachDB는 간혹 DML 수행후 COMMIT시 아래 에러가 발생할 수 있음.
        에러 발생시 RETRY를 1번 정도 다시 수행해 주면 해결 됨. (CockroachDB Document의 Transaction 문서 참조)
        org.postgresql.util.PSQLException: ERROR: restart transaction: TransactionRetryWithProtoRefreshError:
   */
    private void executeBatchWithRetry(Vector<List<Column>> data) throws Exception {

        for (int i=1; i <= DBINFO.MAX_RETRY; i++) {

            try {
                executeBatch(data);
                break;
            } catch (Exception e) {
                _con.rollback();
                EasyLogger.info("# DML Retry 5seconds After!");
                CommonUtil.sleep(5);

                if (DBINFO.MAX_RETRY == i)
                    throw e;
            }
        }
    }

    @Override
    public int runBatch(ResultSet rset, int nMaxCount) throws Exception {
        int nLoop = 0;
        int nRows = 0;

        Vector<List<Column>> dataVec = new Vector<>();
        List<Column> columnList;
        Column colObj;

        while (rset.next()) {

            nLoop++;

            int idx = 1;
            columnList = new ArrayList<>();

            for (Column col:_columnVec) {

                colObj = new Column();
                colObj.setDataType(col.getDataType());

                switch (col.getDataType()){
                    case  STRING:
                        colObj.setDataValue(rset.getString(idx));
                        break;
                    case INTEGER:
                        colObj.setDataValue(Integer.toString(rset.getInt(idx)));
                        break;
                    case DOUBLE:
                        //_pstmt.setDouble(idx, Double.parseDouble(col.getDataValue()));
                        break;
                    case DATE:
                        /*if(col.getDataValue().equalsIgnoreCase("SYSDATE")) {
                            _pstmt.setTimestamp(idx, DateUtil.getTimestamp());
                        }
                        */
                        break;
                    default:
                        throw new Exception("Undefined DATA_TYPE = " + col.getDataType().toString());
                }

                idx++;
                columnList.add(colObj);
            }

            dataVec.add(columnList);

            if (nMaxCount == nLoop) {
                nRows += nMaxCount;

                EasyLogger.info("executeBatch()" + nLoop + " Rows, Total="+nRows);

                executeBatchWithRetry(dataVec);
                dataVec.clear();

                nLoop = 0;
            }
        }

        if (0 < nLoop) {
            EasyLogger.info("Last executeBatch()" + nLoop + " Rows.");
            executeBatchWithRetry(dataVec);
            dataVec.clear();
        }

        return nRows + nLoop;
    }



}
