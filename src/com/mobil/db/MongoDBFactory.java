package com.mobil.db;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mobil.bean.UpdateBean;
import com.mobil.param.ETLParam;
import com.mobil.param.IndexCmdParam;
import com.mobil.param.RunParam;
import com.mobil.util.DataConverter;
import com.mobil.util.DataUtil;
import com.mobil.util.EasyLogger;
import com.mongodb.*;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.*;

import com.mobil.db.Interface.IDataSource;

/**
 * Created by user on 2018-03-19.
 * Author: moonsun kim
 */
public class MongoDBFactory implements IDataSource {

    private MongoClient _mongoc = null;
    private MongoDatabase _db = null;

    @Override
    public boolean connect(String dbName, String userId, String userPasswd) {

        try {

            String strUri = String.format("mongodb://%s:%s@1.1.1.1:30000/?authSource=%s", userId, userPasswd, dbName);

            MongoClientURI uri = new MongoClientURI(strUri);

            _mongoc = new MongoClient(uri);

            System.out.println("MongoDB Connect Success");

        } catch (Exception e) {
            System.out.println("MongoDB Connect Fail");
            e.printStackTrace();
            return false;
        }

        return true;
    }

    @Override
    public boolean disConnect() {

        try {
            _mongoc.close();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    @Override
    public Vector<ETLParam> getEtlParams(RunParam _runParam) throws Exception {
        Vector<ETLParam> vecEtlParams = new Vector<>();

        setUseDatebase("mobdw");

        MongoCollection<Document> etlObject = getCollection("tb_etl_object");

        StringBuilder sbFindCond = new StringBuilder();
        sbFindCond.append( String.format("{etl_mode:'%s', use_yn:'Y' ", _runParam.getEtlMode()));
        if (_runParam.getStrObjId() != null && _runParam.getStrObjId().length() > 0) {
            sbFindCond.append(String.format(", obj_id:'%s'", _runParam.getStrObjId()));
        }
        sbFindCond.append("}");

        System.out.println("strFindCond >> " + sbFindCond.toString());


        ETLParam etlParam;
        AggregateIterable<Document> out;

        try {

            out = etlObject.aggregate(
                    Arrays.asList(
                            Aggregates.match(Document.parse(sbFindCond.toString())),
                            Aggregates.sort(Sorts.ascending("exec_seq_no"))
                    )
            );



            String strExecSql;
            String strCondJson;

            for(Document doc:out) {

                  /*
                     SQL에 미리 정의된 문자열 치환 - 시작일자, 종료일자
                   */
                strExecSql = DataUtil.getNullData(doc.getString("exec_sql"));
                strExecSql = strExecSql.replaceAll("[{]START_DT[}]", _runParam.getStrStDt());
                strExecSql = strExecSql.replaceAll("[{]END_DT[}]", _runParam.getStrEdDt());

                strCondJson = DataUtil.getNullData(doc.getString("load_cond_clause"));
                strCondJson = strCondJson.replaceAll("[{]START_DT[}]", _runParam.getStrStDt());
                strCondJson = strCondJson.replaceAll("[{]END_DT[}]", _runParam.getStrEdDt());
                strCondJson = strCondJson.replaceAll("[{]YYYYMM[}]", _runParam.getStrStDt().substring(0,6));

                etlParam = new ETLParam();

                etlParam.setYmd(_runParam.getStrStDt().substring(0,6));
                etlParam.setExecSql(strExecSql);
                etlParam.setLoadCondClause(strCondJson);
                etlParam.setObjId(doc.getString("obj_id"));
                etlParam.setTargetTableNm(doc.getString("mongo_clt_nm").replaceAll("[{]YYYYMM[}]", etlParam.getYmd()));
                etlParam.setLoadType(doc.getString("load_type"));

                vecEtlParams.add(etlParam);

            }

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

        return vecEtlParams;
    }

    @Override
    public Vector<IndexCmdParam> getIndexCommands(ETLParam _etlParam) throws Exception {
        Vector<IndexCmdParam> vecIdxCmds = new Vector<>();

        setUseDatebase("mobdw");

        MongoCollection<Document> etObjIdx = getCollection("tb_etl_object_index");

        ETLParam etlParam;
        AggregateIterable<Document> out;

        try {

            out = etObjIdx.aggregate(
                    Arrays.asList(
                            Aggregates.match(Document.parse("{obj_id:'" + _etlParam.getObjId() + "', use_yn:'Y'}")),
                            Aggregates.sort(Sorts.ascending("seq_no"))
                    )
            );

            String strExecCode;

            IndexCmdParam idxParam;

            for(Document doc:out) {

                idxParam = new IndexCmdParam();

                strExecCode = doc.getString("exec_code");
                strExecCode = strExecCode.replaceAll("[{]YYYYMM[}]", _etlParam.getYmd());

                idxParam.setSeqNo(doc.getInteger("seq_no"));
                idxParam.setStrCommand(strExecCode);

                System.out.printf("runCommand = [%s] \n", strExecCode);

                vecIdxCmds.add(idxParam);

            }

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

        return vecIdxCmds;
    }

    @Override
    public Object getConnection() {
        return _db;
    }

    public boolean setUseDatebase(String dbName) {

        _db = null;

        try {
            _db = _mongoc.getDatabase(dbName);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    public boolean isExistCollection(String strColName) throws Exception {

        if(_db.getCollection(strColName).count() > 0)
            return true;
        else
            return false;
    }

    public MongoCollection<Document> getCollection(String strColName) {

        return _db.getCollection(strColName);
    }

    public Vector<String> getCollectionNames() throws Exception {

        Vector<String> namesVec = new Vector<>();

        MongoIterable<String> cols = _db.listCollectionNames();

        if (cols == null)
            throw new Exception("Get Collection Names Fail");

        for (String col : cols) {
            namesVec.add(col);
        }

        return namesVec;
    }

    public boolean runCommand(String strJsonCmd) throws Exception {

        Document doc = Document.parse(strJsonCmd);

        _db.runCommand(doc);

        return true;
    }

    public boolean insertMany(String collectionName, JsonArray jsonData) throws Exception {

        MongoCollection<Document> collection = _db.getCollection(collectionName);

        List<Document> data = DataConverter.jsonToList(jsonData);

        collection.insertMany(data);

        return true;
    }

    public long deleteMany(String collectionName, Document doc) throws Exception {

        MongoCollection<Document> collection = _db.getCollection(collectionName);

        DeleteResult res = collection.deleteMany(doc);

        return res.getDeletedCount();
    }

    /*
          하나의 조건에 하나 이상의 다큐먼트를 갱신할 경우
         update xxx
           set a = 100
         where aa = 1
           and bb = 1

     */
    public long updateMany(String collectionName, HashMap<String, Object> keyMap, String strJson) throws Exception {

        MongoCollection<Document> collection = _db.getCollection(collectionName);

        BasicDBObject query = new BasicDBObject();

        Set set = keyMap.entrySet();

        for (Object aSet : set) {
            Map.Entry mentry = (Map.Entry) aSet;
            query.append((String) mentry.getKey(), mentry.getValue());
        }

        Document doc = Document.parse(strJson);
        Bson data = new Document("$set", doc);

        UpdateResult result = collection.updateMany(query, data, new UpdateOptions().upsert(false));

        return result.getMatchedCount();
    }

    /*
        업데이트하고자 하는 다수의 다큐먼트를 한번에 갱신하고자 할때 사용
         update xxx
           set a = b.x
         from xxx x, yyy y
         where  x.a = b.a

        dataMap<updateKeys, updateDocuments> ...
     */
     public long bulkUpdate(String collectionName, HashMap<JsonObject, JsonObject> dataMap) throws Exception {

        long nRows;

        MongoCollection<Document> collection = _db.getCollection(collectionName);

        List<WriteModel<Document>> updateDocuments = new ArrayList<>();

        Set set = dataMap.entrySet();
        Iterator iter = set.iterator();

        JsonObject filterJo;
        JsonObject updateJo;

        Document filterDoc;
        Document setDoc;
        Document updateDoc;

        UpdateOptions updateOptions = new UpdateOptions();
        updateOptions.upsert(false);
        updateOptions.bypassDocumentValidation(true);

        while(iter.hasNext()) {
            Map.Entry mentry = (Map.Entry)iter.next();
            filterJo = (JsonObject)mentry.getKey();
            updateJo = (JsonObject)mentry.getValue();

            filterDoc  = Document.parse(filterJo.toString());
            setDoc     = Document.parse(updateJo.toString());

            updateDoc  = new Document();
            updateDoc.append("$set", setDoc);

            updateDocuments.add(
                    new UpdateManyModel<>(
                            filterDoc
                           ,updateDoc
                           ,updateOptions));
        }

        BulkWriteOptions bulkWriteOptions = new BulkWriteOptions();
        bulkWriteOptions.ordered(false); // false = parallel, true = serial (1 thread)
        bulkWriteOptions.bypassDocumentValidation(false);

        BulkWriteResult bulkWriteResult;
        try {
            //Perform bulk update
            bulkWriteResult = collection.bulkWrite(updateDocuments, bulkWriteOptions);

        } catch (BulkWriteException e) {
            //Handle bulkwrite exception
            List<BulkWriteError> bulkWriteErrors = e.getWriteErrors();
            for (BulkWriteError bulkWriteError : bulkWriteErrors) {
                int failedIndex = bulkWriteError.getIndex();
                EasyLogger.error("Failed record: " +failedIndex);
            }

            throw e;
        }

        nRows = bulkWriteResult.getModifiedCount();

        return nRows;
    }

    /*
        $set, $inc 연산자를 미리 JsonObject에 담아 업데이트하는 함수.

        => UpdateOne or UpdateMany 옵션처리할 것.. (추후)
     */
    public long bulkUpdateByBean(String collectionName, UpdateBean upBean) throws Exception {

        long nRows;

        MongoCollection<Document> collection = _db.getCollection(collectionName);

        List<WriteModel<Document>> updateDocuments = new ArrayList<>();

        Document filterDoc;
        Document updateDoc;

        UpdateOptions updateOptions = new UpdateOptions();
        updateOptions.upsert(false);
        updateOptions.bypassDocumentValidation(true);

        int nSize = upBean.size();

        for(int i = 0; i < nSize; i++) {

            filterDoc  = Document.parse(upBean.getFilterJo(i).toString());
            updateDoc  = Document.parse(upBean.getUpdateJo(i).toString());

            updateDocuments.add(
                    new UpdateManyModel<>(
                            filterDoc
                            ,updateDoc
                            ,updateOptions));
        }

        BulkWriteOptions bulkWriteOptions = new BulkWriteOptions();
        bulkWriteOptions.ordered(false); // false = parallel, true = serial (1 thread)
        bulkWriteOptions.bypassDocumentValidation(false);

        BulkWriteResult bulkWriteResult;
        try {
            //Perform bulk update
            bulkWriteResult = collection.bulkWrite(updateDocuments, bulkWriteOptions);

        } catch (BulkWriteException e) {
            //Handle bulkwrite exception
            List<BulkWriteError> bulkWriteErrors = e.getWriteErrors();
            for (BulkWriteError bulkWriteError : bulkWriteErrors) {
                int failedIndex = bulkWriteError.getIndex();
                System.out.println("Failed record: " +failedIndex);
            }

            throw e;
        }

        nRows = bulkWriteResult.getModifiedCount();

        return nRows;
    }


}
