package com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.field.groupby.GroupBy;

import java.util.List;

public class GroupByFields {
    private List<GroupBy> _fields;

    public List<GroupBy> get_fields() {
        return _fields;
    }

    public void set_fields(List<GroupBy> fields) {
        this._fields = fields;
    }
}
