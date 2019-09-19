package com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.field.source;

import com.moumou.bootmybatisdemo.serviceinterfaceimp.layer.model.field.Direct;

public class DirectField extends SourceField implements Direct {
    private String _tableAsName;
    private String _fieldName;

    public String get_tableAsName() {
        return _tableAsName;
    }

    public void set_tableAsName(String tableAsName) {
        this._tableAsName = tableAsName;
    }

    @Override
    public String get_fieldName() {
        return this._fieldName;
    }

    @Override
    public void set_fieldName(String fieldName) {
        this._fieldName = fieldName;
    }
}
