package com.mobil.bean;

import com.google.gson.JsonObject;

import java.util.Vector;


public class UpdateBean {
    private Vector<JsonObject> filterVec;
    private Vector<JsonObject> updateVec;


    public UpdateBean() {
        filterVec = new Vector<>();
        updateVec = new Vector<>();
    }

    public void clear() throws Exception{
        filterVec.clear();
        updateVec.clear();
    }

    public int size() {
        return filterVec.size();
    }

    public boolean add(JsonObject filterJo, JsonObject updateJo) throws Exception{
        filterVec.add(filterJo);
        updateVec.add(updateJo);
        return true;
    }

    public JsonObject getFilterJo(int i) {
        return filterVec.get(i);
    }

    public JsonObject getUpdateJo(int i) {
        return updateVec.get(i);
    }

}
