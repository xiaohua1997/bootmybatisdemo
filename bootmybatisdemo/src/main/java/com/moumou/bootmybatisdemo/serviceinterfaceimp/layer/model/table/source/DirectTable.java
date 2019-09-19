package com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.table.source;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.table.Direct;

public class DirectTable extends SourceTable implements Direct {
    private String _schema;
    private String _tableName;

    @Override
    public String get_schema() {
        return _schema;
    }

    @Override
    public void set_schema(String schema) {
        this._schema = schema;
    }

    @Override
    public String get_tableName() {
        return _tableName;
    }

    @Override
    public void set_tableName(String tableName) {
        this._tableName = tableName;
    }
}
