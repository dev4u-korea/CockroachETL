package com.mobil.db;

import java.sql.*;
import java.util.Vector;

import com.mobil.constant.DBINFO;
import com.mobil.db.Interface.IDataSource;
import com.mobil.param.ETLParam;
import com.mobil.param.IndexCmdParam;
import com.mobil.param.RunParam;
import com.mobil.util.CommonUtil;
import com.mobil.util.DataUtil;
import com.mobil.util.EasyLogger;

public class CockroachDBFactory implements IDataSource {

    private Connection _con = null;

    @Override
    public boolean connect(String dbName, String userId, String userPasswd) {

        try {

            Class.forName("org.postgresql.Driver");

            _con = DriverManager.getConnection("jdbc:postgresql://1.1.1.1:26257/" + dbName +"",userId,userPasswd); // insecure mode

            if(_con != null) {
                System.out.println("CockroachDB Connect Success");
            } else {
                System.out.println("CockroachDB Connect Fail");
            }

            _con.setAutoCommit(false);
            setTimeZone();

        } catch(Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    public void setTimeZone() {

        Statement stmt = null;

        try {
            stmt = _con.createStatement();
            stmt.execute("set time zone 7");
        } catch(Exception e) {
          e.printStackTrace();
        } finally {
            if (stmt != null) try { stmt.close();} catch (Exception e) {e.printStackTrace();}
        }

    }

    @Override
    public boolean disConnect() {

        if (_con != null) {

            try {
                _con.close();
            } catch(Exception e) {
                e.printStackTrace();
                return false;
            }
        }

        return true;
    }

    @Override
    public Object getConnection() {
        return _con;
    }

    public void commit() throws Exception {
        _con.commit();
    }

    public void rollback() throws Exception {
        _con.rollback();
    }

    @Override
    public Vector<ETLParam> getEtlParams(RunParam _runParam) throws Exception {

        Vector<ETLParam> vecEtlParams = new Vector<>();

        StringBuilder sbQry = new StringBuilder();
        sbQry.append("SELECT OBJ_ID                                                              \n");
        sbQry.append("      ,EXEC_SQL                                                            \n");
        sbQry.append("      ,TARGET_TABLE_NM                                                     \n");
        sbQry.append("      ,LOAD_TYPE                                                           \n");
        sbQry.append("      ,LOAD_COND_CLAUSE                                                    \n");
        sbQry.append("FROM MOBDW.TB_ETL_OBJECT                                                   \n");
        sbQry.append("WHERE USE_YN = 'Y'                                                         \n");
        sbQry.append("  AND ETL_MODE = '").append(_runParam.getEtlMode().toString()).append("'   \n");
        if (_runParam.getStrObjId() != null && _runParam.getStrObjId().length() > 0) {
            sbQry.append("AND OBJ_ID = '").append(_runParam.getStrObjId()).append("'             \n");
        }
        sbQry.append("ORDER BY EXEC_SEQ_NO                                                       \n");

        System.out.println("sql = " + sbQry.toString());

        Statement stmt = null;
        ResultSet rset = null;
        ETLParam etlParam;

        try {
            stmt = _con.createStatement();
            rset = stmt.executeQuery(sbQry.toString());

            String strExecSql;
            String strCondJson;

            while (rset.next()) {

                // SQL에 미리 정의된 문자열 치환 - 시작일자, 종료일자

                strExecSql = DataUtil.getNullData(rset.getString("EXEC_SQL"));
                strExecSql = strExecSql.replaceAll("[{]START_DT[}]", _runParam.getStrStDt());
                strExecSql = strExecSql.replaceAll("[{]END_DT[}]", _runParam.getStrEdDt());

                strCondJson = DataUtil.getNullData(rset.getString("LOAD_COND_CLAUSE"));
                strCondJson = strCondJson.replaceAll("[{]START_DT[}]", _runParam.getStrStDt());
                strCondJson = strCondJson.replaceAll("[{]END_DT[}]", _runParam.getStrEdDt());

                etlParam = new ETLParam();

                etlParam.setYmd(_runParam.getStrStDt().substring(0, 6));
                etlParam.setExecSql(strExecSql);
                etlParam.setLoadCondClause(strCondJson);
                etlParam.setObjId(rset.getString("OBJ_ID"));
                //etlParam.setMongoDbNm(rset.getString("MONGO_DB_NM"));
                etlParam.setTargetTableNm(rset.getString("TARGET_TABLE_NM").replaceAll("[{]YYYYMM[}]", etlParam.getYmd()));
                etlParam.setLoadType(rset.getString("LOAD_TYPE"));

                vecEtlParams.add(etlParam);

            }

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if(rset  != null) try { rset.close();  } catch (Exception ig){ ig.printStackTrace();}
            if(stmt != null) try { stmt.close(); } catch (Exception ig){ ig.printStackTrace();}
        }

        return vecEtlParams;
    }

    @Override
    public Vector<IndexCmdParam> getIndexCommands(ETLParam _etlParam) throws Exception {

        return null;
    }

    private boolean truncate(String strTableNm) throws Exception {

        Statement stmt = _con.createStatement();

        try {

            stmt.execute("truncate table " + strTableNm);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (stmt != null) try { stmt.close();} catch (Exception ig) {}
        }

        return true;
    }

    public boolean truncateWithRetry(String strTableNm) throws Exception {

        boolean bRet = false;

        for (int i=1; i <=  DBINFO.MAX_RETRY; i++) {

            try {
                bRet = truncate(strTableNm);
                break;
            } catch (Exception e) {
                _con.rollback();
                EasyLogger.info("# DML Retry 5seconds After!");
                CommonUtil.sleep(5);

                if (DBINFO.MAX_RETRY == i)
                    return false;
            }
        }

        return true;
    }

    private long delete(String strTableNm, String strDeleteCondition) throws Exception {

        Statement stmt = _con.createStatement();
        long nRows = 0;
        int nCurRows;

        try {
             do {
                  nCurRows = stmt.executeUpdate("delete from " + strTableNm + " where " + strDeleteCondition + " limit " + (DBINFO.MAX_SIZE * 10));
                  nRows += nCurRows;

                  _con.commit();
             } while (nCurRows >  0);

        } catch (Exception e) {
            _con.rollback();
            throw e;
        } finally {
            if (stmt != null) try { stmt.close();} catch (Exception ig) {}
        }

        return nRows;
    }

    public long deleteWithRetry(String strTableNm, String strDeleteCondition) throws Exception {

        long nRows = 0;

        for (int i=1; i <=  DBINFO.MAX_RETRY; i++) {

            try {
                nRows += delete(strTableNm, strDeleteCondition);
                break;
            } catch (Exception e) {
                _con.rollback();
                EasyLogger.info("# DML Retry 5seconds After!");
                CommonUtil.sleep(5);

                if (DBINFO.MAX_RETRY == i)
                    nRows = -1;
            }
        }

        return nRows;
    }
}
