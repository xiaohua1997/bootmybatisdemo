package com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.field.source.SourceField;

import java.util.List;

public class SelectFields {
    private List<SourceField> _fields;

    public List<SourceField> get_fields() {
        return _fields;
    }

    public void set_fields(List<SourceField> fields) {
        this._fields = fields;
    }
}
