package com.mobil.util;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.mobil.db.object.Column;
import com.mobil.type.DATA_TYPE;
import org.bson.Document;

import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

/**
 * Created by user on 2018-03-22.
 * Author: moonsun kim
 */
public class DataConverter {

    /*
        ResultSet to JsonArray
     */
    public static JsonArray rsetToJson4Insert(ResultSet rset, int maxSize) throws Exception {

        JsonArray         jsonData = new JsonArray();
        ResultSetMetaData rmeta;
        JsonObject        jo;

        rmeta = rset.getMetaData();

        int nRows = 0;

        while(rset.next()) {
            nRows++;

            jo = new JsonObject();
            for(int i=1; i <= rmeta.getColumnCount(); i++) {

                if (rmeta.getColumnTypeName(i).equals("NUMBER")) {
                    try {
                        jo.addProperty(rmeta.getColumnName(i).toLowerCase(), rset.getInt(i));
                    } catch (Exception ig){
                        jo.addProperty(rmeta.getColumnName(i).toLowerCase(), rset.getDouble(i));
                    }
                } else {
                    jo.addProperty(rmeta.getColumnName(i).toLowerCase(), rset.getString(i));
                }

            }

            jsonData.add(jo);

            //if(nRows == 1) System.out.println("first rows = " + jo.toString());

            if (maxSize == nRows)
                break;
        }

        return jsonData;
    }

    /*
        param: keyIndex = 업데이트 조건과 업데이트 대상를 구분하는 값
     */
    public static HashMap<JsonObject, JsonObject> rsetToJson4Update(ResultSet rset, int keyIndex, int maxSize) throws Exception {

        HashMap<JsonObject, JsonObject> dataMap = new HashMap<>();

        ResultSetMetaData rmeta;

        JsonObject  filterJo;
        JsonObject  updateJo;

        rmeta = rset.getMetaData();

        int nRows = 0;

        while(rset.next()) {
            nRows++;

            filterJo = new JsonObject();
            updateJo = new JsonObject();

            for(int i=1; i <= rmeta.getColumnCount(); i++) {

                if (i <= keyIndex ) {
                    if (rmeta.getColumnTypeName(i).equals("NUMBER")) {
                        try {
                            filterJo.addProperty(rmeta.getColumnName(i).toLowerCase(), rset.getInt(i));
                        } catch (Exception ig) {
                            filterJo.addProperty(rmeta.getColumnName(i).toLowerCase(), rset.getDouble(i));
                        }
                    } else {
                        filterJo.addProperty(rmeta.getColumnName(i).toLowerCase(), rset.getString(i));
                    }
                } else {
                    if (rmeta.getColumnTypeName(i).equals("NUMBER")) {
                        try {
                            updateJo.addProperty(rmeta.getColumnName(i).toLowerCase(), rset.getInt(i));
                        } catch (Exception ig) {
                            updateJo.addProperty(rmeta.getColumnName(i).toLowerCase(), rset.getDouble(i));
                        }
                    } else {
                        updateJo.addProperty(rmeta.getColumnName(i).toLowerCase(), rset.getString(i));
                    }
                }


            }

            dataMap.put(filterJo, updateJo);

            if (maxSize == nRows)
                break;
        }

        return dataMap;
    }

    /*
        JsonArray(Gson) to List<Document>(bson)
     */
    public static List<Document> jsonToList(JsonArray jsonData) throws Exception {

        Gson parser = new Gson();

        Type listType = new TypeToken<List<Document>>() {}.getType();

        return parser.fromJson(jsonData.toString(), listType);

    }

    public static Vector<Column> getColumnsFromMeta(ResultSetMetaData rmeta) throws Exception {

        Vector<Column> _columnVec = new Vector<>();

        Column col;

        for(int i = 1; i <= rmeta.getColumnCount(); i++) {

            col = new Column();
            col.setColumnName(rmeta.getColumnName(i));

            if (rmeta.getColumnTypeName(i).equals("NUMBER")) {
                col.setDataType(DATA_TYPE.INTEGER);
            } else {
                col.setDataType(DATA_TYPE.STRING);
            }

            _columnVec.add(col);
        }

        return _columnVec;
    }

}
