package com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.table.source;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.table.Table;

public abstract class SourceTable extends Table {
    private String _asName;

    public String get_asName() {
        return _asName;
    }

    public void set_asName(String asName) {
        this._asName = asName;
    }
}
