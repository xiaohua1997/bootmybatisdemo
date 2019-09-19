package com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.table.target;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.field.target.TargetField;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.table.Direct;
import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.table.Table;

import java.util.List;

public class TargetTable extends Table implements Direct {
    private String _schema;
    private String _tableName;
    private List<TargetField> _fields;

    public List<TargetField> get_fields() {
        return _fields;
    }

    public void set_fields(List<TargetField> fields) {
        this._fields = fields;
    }

    @Override
    public String get_schema() {
        return this._schema;
    }

    @Override
    public void set_schema(String schema) {
        this._schema = schema;
    }

    @Override
    public String get_tableName() {
        return this._tableName;
    }

    @Override
    public void set_tableName(String tableName) {
        this._tableName = tableName;
    }
}

