package com.mobil.db.Interface;

import com.mobil.param.ETLParam;
import com.mobil.param.IndexCmdParam;
import com.mobil.param.RunParam;

import java.util.Vector;

/**
 * Created by user on 2018-03-19.
 * Author: moonsun kim
 */
public interface IDataSource {
    boolean connect(String dbName, String userId, String userPasswd);
    boolean disConnect();
    Object  getConnection();
    Vector<ETLParam> getEtlParams(RunParam _runParam) throws Exception;
    Vector<IndexCmdParam> getIndexCommands(ETLParam _etlParam) throws Exception;
}
