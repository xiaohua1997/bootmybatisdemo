package com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.table;

public abstract class Table {
    private String _tableCnName;

    public String get_tableCnName() {
        return _tableCnName;
    }

    public void set_tableCnName(String tableCnName) {
        this._tableCnName = tableCnName;
    }
}
